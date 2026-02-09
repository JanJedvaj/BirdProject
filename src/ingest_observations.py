from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple, List

from dotenv import load_dotenv
from pymongo import MongoClient, ASCENDING
from pymongo.errors import PyMongoError


KAFKA_CONTAINER = "birdpipe_kafka"


def require_env(name: str) -> str:
    v = os.getenv(name)
    if not v:
        raise RuntimeError(f"Missing required env var: {name}")
    return v


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def safe_json(line: str) -> Dict[str, Any]:
    try:
        return json.loads(line)
    except Exception:
        return {"_raw": line}


def extract_taxon_and_geo(payload: Dict[str, Any]) -> Tuple[Optional[str], Optional[float], Optional[float]]:
    taxon = (
        payload.get("taxon_code")
        or payload.get("taxonCode")
        or payload.get("species_code")
        or payload.get("speciesCode")
        or payload.get("taxon")
        or payload.get("species")
        or payload.get("code")
    )

    lat = payload.get("lat") or payload.get("latitude")
    lon = payload.get("lon") or payload.get("lng") or payload.get("longitude") or payload.get("long")

    loc = payload.get("location") or payload.get("geo") or payload.get("coordinates")
    if isinstance(loc, dict):
        lat = lat or loc.get("lat") or loc.get("latitude")
        lon = lon or loc.get("lon") or loc.get("lng") or loc.get("longitude")

    def to_float(x: Any) -> Optional[float]:
        try:
            return None if x is None else float(x)
        except Exception:
            return None

    return (str(taxon) if taxon is not None else None, to_float(lat), to_float(lon))


def ensure_docker_and_container() -> None:
    try:
        subprocess.run(["docker", "version"], capture_output=True, text=True, check=True)
    except Exception as ex:
        raise RuntimeError("Docker does not seem to be running or accessible (docker version failed).") from ex

    res = subprocess.run(["docker", "ps", "--format", "{{.Names}}"], capture_output=True, text=True)
    names = [n.strip() for n in res.stdout.splitlines() if n.strip()]
    if KAFKA_CONTAINER not in names:
        raise RuntimeError(
            f"Kafka container '{KAFKA_CONTAINER}' is not running. Running containers: {names}"
        )


def _run_consumer_once(topic: str, timeout_ms: int) -> str:
    consumer_cmd = (
        "kafka-console-consumer "
        "--bootstrap-server localhost:9092 "
        f"--topic {topic} "
        "--from-beginning "
        f"--timeout-ms {timeout_ms}"
    )
    cmd = ["docker", "exec", "-i", KAFKA_CONTAINER, "bash", "-lc", consumer_cmd]

    # IMPORTANT FIX: some output can land on stderr (TTY/buffering quirks)
    proc = subprocess.run(cmd, capture_output=True, text=True)
    combined = ""
    if proc.stdout:
        combined += proc.stdout
    if proc.stderr:
        combined += proc.stderr
    return combined


def consume_kafka_lines(topic: str, timeout_ms: int) -> List[str]:
    """
    Single-shot consume: run console-consumer once and collect stdout+stderr.
    This avoids infinite loops caused by broker log/warn lines.
    """
    consumer_cmd = (
        "kafka-console-consumer "
        "--bootstrap-server localhost:9092 "
        f"--topic {topic} "
        "--from-beginning "
        f"--timeout-ms {timeout_ms}"
    )
    cmd = ["docker", "exec", "-i", KAFKA_CONTAINER, "bash", "-lc", consumer_cmd]

    proc = subprocess.run(cmd, capture_output=True, text=True)

    combined = ""
    if proc.stdout:
        combined += proc.stdout
    if proc.stderr:
        combined += proc.stderr

    lines = [ln.strip() for ln in combined.splitlines() if ln.strip()]

    # Filter obvious kafka log lines if they appear
    filtered = []
    for ln in lines:
        if ln.startswith("[") and "kafka." in ln:
            continue
        filtered.append(ln)

    return filtered



def main() -> int:
    load_dotenv()

    mongo_uri = require_env("MONGO_URI")
    db_name = require_env("MONGO_DB")
    topic = require_env("KAFKA_TOPIC")

    max_seconds = int(os.getenv("KAFKA_MAX_SECONDS", "8"))
    timeout_ms = int(os.getenv("KAFKA_TIMEOUT_MS", "5000"))

    print(f" Ingest starting via Docker consumer (container={KAFKA_CONTAINER}, topic={topic})")

    try:
        ensure_docker_and_container()

        client = MongoClient(mongo_uri, serverSelectionTimeoutMS=8000)
        client.admin.command("ping")
        db = client[db_name]
        col = db["field_observations"]

        col.create_index([("ingested_at", ASCENDING)])
        col.create_index([("taxon_code", ASCENDING)])
        col.create_index([("topic", ASCENDING), ("seq", ASCENDING)], unique=True)

        lines = consume_kafka_lines(topic, timeout_ms=timeout_ms)


        # Optional: filter out obvious log lines if they appear
        filtered = []
        for ln in lines:
            if ln.startswith("[") and "kafka." in ln:
                continue
            filtered.append(ln)

        print(f" Kafka output lines collected: {len(filtered)}")

        ingested = 0
        for i, line in enumerate(filtered):
            payload = safe_json(line)
            taxon_code, lat, lon = extract_taxon_and_geo(payload)

            doc = {
                "source": "kafka",
                "topic": topic,
                "seq": i,
                "ingested_at": now_utc(),
                "taxon_code": taxon_code,
                "lat": lat,
                "lon": lon,
                "payload": payload,
            }

            col.update_one(
                {"topic": topic, "seq": i},
                {"$setOnInsert": doc},
                upsert=True,
            )
            ingested += 1

        print(f" Ingest done. Mongo upsert attempts: {ingested}")
        print(" Data stored in Mongo collection: field_observations")
        return 0

    except PyMongoError as ex:
        print(f" Mongo error: {ex}", file=sys.stderr)
        return 1
    except Exception as ex:
        print(f" Ingest failed: {ex}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
