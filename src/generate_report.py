from __future__ import annotations

import argparse
import csv
import os
import re
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from dotenv import load_dotenv
from pymongo import MongoClient


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def require_env(name: str) -> str:
    v = os.getenv(name)
    if not v:
        raise RuntimeError(f"Missing env var: {name}")
    return v


def norm(s: str) -> str:
    s = (s or "").lower().strip()
    s = re.sub(r"\s+", " ", s)
    return s


# --- Fuzzy (trigram Jaccard) ---
def trigram_set(s: str) -> set[str]:
    s = f"  {norm(s)}  "
    if len(s) < 3:
        return set()
    return {s[i : i + 3] for i in range(len(s) - 2)}


def trigram_similarity(a: str, b: str) -> float:
    A, B = trigram_set(a), trigram_set(b)
    if not A or not B:
        return 0.0
    return len(A & B) / len(A | B)


def fuzzy_match(query: str, candidates: List[str], min_score: float = 0.35) -> set[str]:
    scored = [(c, trigram_similarity(query, c)) for c in candidates]
    scored.sort(key=lambda x: x[1], reverse=True)
    return {c for c, s in scored if s >= min_score}


def safe_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        return float(x)
    except Exception:
        return None


def main() -> int:
    load_dotenv()

    mongo_uri = require_env("MONGO_URI")
    db_name = require_env("MONGO_DB")

    out_dir = os.getenv("OUT_DIR", r".\out")
    os.makedirs(out_dir, exist_ok=True)

    ap = argparse.ArgumentParser(description="Project 2 - Step 4 CSV report (from classifications raw.results)")
    ap.add_argument("--species", type=str, default=None, help="Optional fuzzy filter by species name")
    ap.add_argument(
        "--min-score",
        type=float,
        default=float(os.getenv("POSITIVE_THRESHOLD", "0.30")),
        help="Confidence threshold for 'positive segment' (default 0.30)",
    )
    ap.add_argument("--top-n", type=int, default=0, help="Top N rows (0 = all)")
    args = ap.parse_args()

    threshold = float(args.min_score)

    client = MongoClient(mongo_uri, serverSelectionTimeoutMS=8000)
    client.admin.command("ping")
    db = client[db_name]

    cls_col = db["classifications"]
    obs_col = db["field_observations"]

    # Pull all classifications
    cls_docs = list(cls_col.find({}, {"_id": 0, "audio_id": 1, "geo": 1, "raw": 1, "best_label": 1}))
    print(f"Loaded classifications: {len(cls_docs)}")

    # Aggregate by species label
    # Key strategy:
    # - Prefer scientific_name if present
    # - Else label
    agg: Dict[str, Dict[str, Any]] = {}
    seen_audio_per_species: Dict[str, set[str]] = defaultdict(set)

    for doc in cls_docs:
        audio_id = doc.get("audio_id")
        geo = doc.get("geo") or {}
        results = None

        raw = doc.get("raw") or {}
        if isinstance(raw, dict):
            results = raw.get("results")

        if not isinstance(results, list) or not results:
            # no results => nothing to report from this doc
            continue

        for r in results:
            if not isinstance(r, dict):
                continue

            label = r.get("label") or doc.get("best_label") or "unknown"
            scientific = r.get("scientific_name") or ""
            common = r.get("common_name") or ""

            confidence = safe_float(r.get("confidence"))
            if confidence is None:
                continue

            # species_key: use scientific name if available because it's stable & clean
            species_key = scientific.strip() if scientific.strip() else str(label).strip()

            entry = agg.setdefault(
                species_key,
                {
                    "scientific_name": scientific.strip(),
                    "common_name": common.strip(),
                    "label": str(label).strip(),
                    "positive_segments": 0,
                    "segments_total": 0,
                    "confidence_sum": 0.0,
                    "confidence_n": 0,
                    "max_confidence": 0.0,
                    "sample_lat": None,
                    "sample_lon": None,
                },
            )

            entry["segments_total"] += 1
            entry["confidence_sum"] += float(confidence)
            entry["confidence_n"] += 1
            if float(confidence) > entry["max_confidence"]:
                entry["max_confidence"] = float(confidence)

            if float(confidence) >= threshold:
                entry["positive_segments"] += 1

            if entry["sample_lat"] is None:
                entry["sample_lat"] = geo.get("lat")
                entry["sample_lon"] = geo.get("lon")

            if audio_id:
                seen_audio_per_species[species_key].add(str(audio_id))

    # Only species with at least one positive segment 
    rows: List[Dict[str, Any]] = []
    for species_key, e in agg.items():
        if e["positive_segments"] <= 0:
            continue

        avg_conf = (e["confidence_sum"] / e["confidence_n"]) if e["confidence_n"] else 0.0

        # "Relevant observation data" from Kafka observations:
        obs_count = 0
        # Attempt: taxon_code == species_key (rare) or taxon_code == label
        obs_count = obs_col.count_documents({"taxon_code": species_key})
        if obs_count == 0 and e.get("label"):
            obs_count = obs_col.count_documents({"taxon_code": e["label"]})

        rows.append(
            {
                "scientific_name": e["scientific_name"] or species_key,
                "common_name": e["common_name"],
                "label": e["label"],
                "audio_files_count": len(seen_audio_per_species.get(species_key, set())),
                "positive_segments": int(e["positive_segments"]),
                "segments_total": int(e["segments_total"]),
                "max_confidence": round(float(e["max_confidence"]), 4),
                "avg_confidence": round(float(avg_conf), 4),
                "observations_count": int(obs_count),
                "sample_lat": e["sample_lat"],
                "sample_lon": e["sample_lon"],
            }
        )

    # Cleaning / transformation
    for r in rows:
        # normalize empty fields
        if not r["common_name"]:
            r["common_name"] = "(unknown)"
        r["scientific_name"] = r["scientific_name"].strip()
        r["common_name"] = r["common_name"].strip()

    if args.species:
        query = args.species
        candidates = []
        for r in rows:
            candidates.extend([r["scientific_name"], r["common_name"], r["label"]])
        allowed = fuzzy_match(query, candidates, min_score=0.35)

        def is_allowed(r: Dict[str, Any]) -> bool:
            return (
                r["scientific_name"] in allowed
                or r["common_name"] in allowed
                or r["label"] in allowed
                or trigram_similarity(query, r["scientific_name"]) >= 0.35
                or trigram_similarity(query, r["common_name"]) >= 0.35
            )

        rows = [r for r in rows if is_allowed(r)]

    # Sort: most positives first, then max confidence
    rows.sort(key=lambda r: (r["positive_segments"], r["max_confidence"]), reverse=True)

    if args.top_n and args.top_n > 0:
        rows = rows[: args.top_n]

    out_path = os.path.join(out_dir, f"bird_report_{now_utc_iso().replace(':','-')}.csv")

    fieldnames = [
        "scientific_name",
        "common_name",
        "label",
        "audio_files_count",
        "positive_segments",
        "segments_total",
        "max_confidence",
        "avg_confidence",
        "observations_count",
        "sample_lat",
        "sample_lon",
    ]

    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow(r)

    print(f"CSV generated: {out_path}")
    print(f"Rows: {len(rows)} | threshold={threshold} | fuzzy={'ON' if args.species else 'OFF'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
