from __future__ import annotations

import os
import sys
from datetime import datetime, timezone

from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.errors import PyMongoError

from minio import Minio
from minio.error import S3Error


def require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required env var: {name}")
    return value


def check_mongo(mongo_uri: str, db_name: str) -> None:
    print("Checking MongoDB connection...")
    try:
        client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
        # ping server
        client.admin.command("ping")

        db = client[db_name]
        col = db["healthchecks"]

        doc = {
            "kind": "healthcheck",
            "ts": datetime.now(timezone.utc),
            "host": os.getenv("COMPUTERNAME") or "unknown",
        }
        result = col.insert_one(doc)
        print(f"Mongo OK (inserted _id={result.inserted_id})")
    except PyMongoError as ex:
        raise RuntimeError(f"MongoDB healthcheck failed: {ex}") from ex


def check_minio(endpoint: str, access_key: str, secret_key: str, bucket: str) -> None:
    print("Checking MinIO connection...")
    try:
        client = Minio(
            endpoint=endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=False,  # localhost
        )

        # list buckets (forces auth + connectivity)
        buckets = client.list_buckets()
        existing_names = [b.name for b in buckets]

        if bucket not in existing_names:
            client.make_bucket(bucket)
            print(f"MinIO bucket created: {bucket}")
        else:
            print(f"MinIO bucket exists: {bucket}")

        # put a tiny object to verify write access
        content = b"ok"
        object_name = f"healthchecks/{datetime.now(timezone.utc).isoformat().replace(':','-')}.txt"
        from io import BytesIO
        client.put_object(bucket, object_name, BytesIO(content), length=len(content), content_type="text/plain")
        print(f"MinIO OK (uploaded {object_name})")
    except S3Error as ex:
        raise RuntimeError(f"MinIO healthcheck failed: {ex}") from ex


def main() -> int:
    load_dotenv()  # reads .env from project root (current working dir)

    try:
        mongo_uri = require_env("MONGO_URI")
        mongo_db = require_env("MONGO_DB")

        minio_endpoint = require_env("MINIO_ENDPOINT")
        minio_access = require_env("MINIO_ACCESS_KEY")
        minio_secret = require_env("MINIO_SECRET_KEY")
        minio_bucket = require_env("MINIO_BUCKET")

        check_mongo(mongo_uri, mongo_db)
        check_minio(minio_endpoint, minio_access, minio_secret, minio_bucket)

        print("\nALL OK: MongoDB + MinIO are ready.")
        return 0
    except Exception as ex:
        print(f"\nHEALTHCHECK FAILED: {ex}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
