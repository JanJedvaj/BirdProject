# src/upload_audio.py
from __future__ import annotations

import hashlib
import os
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv
from minio import Minio
from minio.error import S3Error
from pymongo import MongoClient
from pymongo.errors import PyMongoError


def require_env(name: str) -> str:
    v = os.getenv(name)
    if not v:
        raise RuntimeError(f"Missing env var: {name}")
    return v


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def main() -> int:
    load_dotenv()

    audio_dir = Path(require_env("AUDIO_DIR")).resolve()
    lat = float(require_env("AUDIO_LAT"))
    lon = float(require_env("AUDIO_LON"))

    minio_endpoint = require_env("MINIO_ENDPOINT")
    minio_access = require_env("MINIO_ACCESS_KEY")
    minio_secret = require_env("MINIO_SECRET_KEY")
    bucket = require_env("MINIO_BUCKET")

    mongo_uri = require_env("MONGO_URI")
    db_name = require_env("MONGO_DB")

    if not audio_dir.exists():
        raise RuntimeError(f"AUDIO_DIR does not exist: {audio_dir}")
    if not audio_dir.is_dir():
        raise RuntimeError(f"AUDIO_DIR is not a directory: {audio_dir}")

    # 1) Mongo
    client = MongoClient(mongo_uri, serverSelectionTimeoutMS=8000)
    client.admin.command("ping")
    db = client[db_name]
    col = db["audio_files"]

    # 2) MinIO
    minio = Minio(
        minio_endpoint,
        access_key=minio_access,
        secret_key=minio_secret,
        secure=False,
    )

    # ensure bucket
    if not minio.bucket_exists(bucket):
        minio.make_bucket(bucket)

    # 3) scan files
    files = [p for p in audio_dir.iterdir() if p.is_file() and p.suffix.lower() in (".wav", ".mp3")]
    print(f"🎧 Found {len(files)} audio files in {audio_dir}")

    uploaded = 0
    skipped = 0

    for p in files:
        audio_id = sha256_file(p)
        ext = p.suffix.lower()
        object_name = f"audio/{audio_id}{ext}"

        if col.find_one({"audio_id": audio_id}):
            print(f"↩️  Skip {p.name} (already in Mongo)")
            skipped += 1
            continue

        # upload file
        try:
            minio.fput_object(bucket, object_name, str(p))
        except S3Error as ex:
            raise RuntimeError(
                f"MinIO upload failed for '{p.name}'. "
                f"Check MINIO_ACCESS_KEY/MINIO_SECRET_KEY and bucket. Details: {ex}"
            ) from ex

        doc = {
            "audio_id": audio_id,
            "filename": p.name,
            "bucket": bucket,
            "object_name": object_name,
            "lat": lat,
            "lon": lon,
            "uploaded_at": now_utc(),
            "source_dir": str(audio_dir),
        }

        col.insert_one(doc)
        print(f"✅ Uploaded {p.name} -> s3://{bucket}/{object_name}")
        uploaded += 1

    print(f"✅ Audio upload finished. uploaded={uploaded}, skipped={skipped}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
