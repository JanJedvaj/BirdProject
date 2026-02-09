# src/classify_audio.py
from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone
from io import BytesIO
from typing import Any, Dict, Optional

import requests
from dotenv import load_dotenv
from minio import Minio
from minio.error import S3Error
from pymongo import MongoClient, ASCENDING
from pymongo.errors import PyMongoError


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def require_env(name: str) -> str:
    v = os.getenv(name)
    if not v:
        raise RuntimeError(f"Missing env var: {name}")
    return v


def safe_json(resp_text: str) -> Dict[str, Any]:
    try:
        return json.loads(resp_text)
    except Exception:
        return {"raw": resp_text}


def main() -> int:
    load_dotenv()

    # Mongo
    mongo_uri = require_env("MONGO_URI")
    db_name = require_env("MONGO_DB")

    # MinIO
    minio_endpoint = require_env("MINIO_ENDPOINT")
    minio_access = require_env("MINIO_ACCESS_KEY")
    minio_secret = require_env("MINIO_SECRET_KEY")
    bucket = require_env("MINIO_BUCKET")

    # Classify
    classify_url = require_env("CLASSIFY_URL")
    timeout = int(os.getenv("CLASSIFY_TIMEOUT", "60"))

    try:
        # connect mongo
        client = MongoClient(mongo_uri, serverSelectionTimeoutMS=8000)
        client.admin.command("ping")
        db = client[db_name]
        audio_col = db["audio_files"]
        taxa_col = db["taxa"]
        cls_col = db["classifications"]

        # indexes (idempotent)
        cls_col.create_index([("audio_id", ASCENDING)], unique=True)
        cls_col.create_index([("created_at", ASCENDING)])
        cls_col.create_index([("best_label", ASCENDING)])

        # connect minio
        minio = Minio(
            minio_endpoint,
            access_key=minio_access,
            secret_key=minio_secret,
            secure=False,
        )

        if not minio.bucket_exists(bucket):
            raise RuntimeError(f"MinIO bucket missing: {bucket}")

        # audio list
        audios = list(audio_col.find({}))
        print(f"🎧 Found {len(audios)} uploaded audio entries in Mongo (audio_files)")

        processed = 0
        skipped = 0

        for a in audios:
            audio_id = a["audio_id"]
            object_name = a["object_name"]
            filename = a.get("filename", object_name)
            lat = a.get("lat")
            lon = a.get("lon")

            # skip if already classified
            if cls_col.find_one({"audio_id": audio_id}, {"_id": 1}):
                print(f"↩️  Skip {filename} (already classified)")
                skipped += 1
                continue

            # 1) download audio from MinIO into memory
            try:
                obj = minio.get_object(bucket, object_name)
                data = obj.read()
                obj.close()
                obj.release_conn()
            except S3Error as ex:
                print(f"❌ MinIO get_object failed for {object_name}: {ex}", file=sys.stderr)
                continue

            # 2) call classify API
            files = {
                # common pattern: file field name is usually "file"
                "file": (filename, BytesIO(data), "application/octet-stream")
            }

            request_meta = {
                "audio_id": audio_id,
                "bucket": bucket,
                "object_name": object_name,
                "filename": filename,
                "lat": lat,
                "lon": lon,
                "classify_url": classify_url,
                "requested_at": now_utc().isoformat(),
            }

            try:
                resp = requests.post(classify_url, files=files, timeout=timeout)
                resp_text = resp.text or ""
            except requests.RequestException as ex:
                resp = None
                resp_text = str(ex)

            response_meta = {
                "status_code": getattr(resp, "status_code", None),
                "ok": bool(resp is not None and resp.ok),
                "received_at": now_utc().isoformat(),
            }

            parsed = safe_json(resp_text)

            # 3) store request+response log in MinIO
            log_doc = {
                "request": request_meta,
                "response": response_meta,
                "parsed": parsed,
            }

            log_key = f"logs/classify/{audio_id}.json"
            try:
                log_bytes = json.dumps(log_doc, ensure_ascii=False, indent=2).encode("utf-8")
                minio.put_object(
                    bucket,
                    log_key,
                    BytesIO(log_bytes),
                    length=len(log_bytes),
                    content_type="application/json",
                )
            except S3Error as ex:
                print(f"⚠️  Could not store log in MinIO ({log_key}): {ex}", file=sys.stderr)

            # 4) normalize + save result in Mongo
            # We don't know exact API schema; we try common shapes.
            best_label: Optional[str] = None
            best_score: Optional[float] = None

            # common schema guesses
            # - {"best": {"label": "...", "score": 0.9}}
            if isinstance(parsed, dict):
                b = parsed.get("best")
                if isinstance(b, dict):
                    best_label = b.get("label") or b.get("taxon_code") or b.get("species")
                    try:
                        best_score = float(b.get("score")) if b.get("score") is not None else None
                    except Exception:
                        best_score = None

                # - {"predictions":[{"label":"...", "score":...}, ...]}
                if best_label is None:
                    preds = parsed.get("predictions") or parsed.get("results")
                    if isinstance(preds, list) and preds:
                        top = preds[0]
                        if isinstance(top, dict):
                            best_label = top.get("label") or top.get("taxon_code") or top.get("species")
                            try:
                                best_score = float(top.get("score")) if top.get("score") is not None else None
                            except Exception:
                                best_score = None

            # taxonomy link (best effort)
            taxon_doc = None
            if best_label:
                taxon_doc = taxa_col.find_one({"taxon_code": best_label}, {"_id": 0, "latin_name": 1, "common_name": 1})

            cls_col.insert_one(
                {
                    "audio_id": audio_id,
                    "filename": filename,
                    "bucket": bucket,
                    "object_name": object_name,
                    "geo": {"lat": lat, "lon": lon},
                    "created_at": now_utc(),
                    "classify_url": classify_url,
                    "minio_log_key": log_key,
                    "http": response_meta,
                    "best_label": best_label,
                    "best_score": best_score,
                    "taxonomy": taxon_doc,  # may be None
                    "raw": parsed,
                }
            )

            print(f"✅ Classified {filename} -> best={best_label} score={best_score} (log={log_key})")
            processed += 1

        print(f"✅ Classification finished. processed={processed}, skipped={skipped}")
        return 0

    except (PyMongoError, RuntimeError) as ex:
        print(f"❌ classify_audio failed: {ex}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
