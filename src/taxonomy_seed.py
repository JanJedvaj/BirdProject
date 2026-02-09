from __future__ import annotations

import os
import re
import sys
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from bs4 import BeautifulSoup
from dotenv import load_dotenv
from pymongo import MongoClient, ASCENDING
from pymongo.errors import PyMongoError

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


def require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required env var: {name}")
    return value


def normalize_spaces(s: str) -> str:
    return re.sub(r"\s+", " ", s).strip()


def make_taxon_code_from_name(name: Optional[str]) -> Optional[str]:
    """Stable pseudo code from latin/canonical name."""
    if not name:
        return None
    s = name.strip().lower()
    s = re.sub(r"\s+", "_", s)
    s = re.sub(r"[^a-z0-9_]+", "", s)
    return s or None


def parse_species_table_from_rendered_html(html: str) -> List[Dict[str, Any]]:
    """
    Parses the rendered taxonomy table from aves.regoch.net (after JS renders it).
    Expected columns (like colleague):
      Scientific Name | Canonical Name | Rank | Family | Order
    We keep only Rank == SPECIES.
    """
    soup = BeautifulSoup(html, "html.parser")

    table = soup.find("table")
    if not table:
        return []

    tbody = table.find("tbody") or table
    rows = tbody.find_all("tr")

    docs: List[Dict[str, Any]] = []
    for tr in rows:
        tds = tr.find_all("td")
        if len(tds) < 5:
            continue

        scientific_with_author = normalize_spaces(tds[0].get_text(strip=True)) or None
        canonical_name = normalize_spaces(tds[1].get_text(strip=True)) or None
        rank = normalize_spaces(tds[2].get_text(strip=True)) or None
        family = normalize_spaces(tds[3].get_text(strip=True)) or None
        order = normalize_spaces(tds[4].get_text(strip=True)) or None

        if not rank or rank.upper() != "SPECIES":
            continue

        latin_name = canonical_name or scientific_with_author
        if not latin_name:
            continue

        taxon_code = make_taxon_code_from_name(latin_name)

        docs.append(
            {
                "taxon_code": taxon_code,
                "common_name": None,  
                "latin_name": latin_name,
                "scientific_name_full": scientific_with_author,
                "rank": "SPECIES",
                "family": family,
                "order": order,
                "source": "aves.regoch.net",
                "seeded_at": datetime.now(timezone.utc),
            }
        )

    return docs


def get_mongo_collection() :
    mongo_uri = require_env("MONGO_URI")
    db_name = require_env("MONGO_DB")

    client = MongoClient(mongo_uri, serverSelectionTimeoutMS=8000)
    client.admin.command("ping")
    db = client[db_name]
    col = db["taxa"]

    col.create_index([("taxon_code", ASCENDING)], unique=False)
    return col


def build_driver() -> webdriver.Chrome:
    options = webdriver.ChromeOptions()
    options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1280,900")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    return webdriver.Chrome(options=options)


def main() -> int:
    load_dotenv()

    taxonomy_url = os.getenv("TAXONOMY_URL", "https://aves.regoch.net/")
    print(f"Taxonomy seed starting (url={taxonomy_url})")

    try:
        col = get_mongo_collection()

        existing = col.estimated_document_count()
        if existing > 0:
            print(f"Skip: taxa already seeded (count≈{existing})")
            return 0

        driver = build_driver()
        wait = WebDriverWait(driver, 30)

        inserted = 0
        try:
            driver.get(taxonomy_url)

            time.sleep(2)

            try:
                wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "table")))
            except Exception:
                wait.until(EC.presence_of_element_located((By.ID, "pageInfo")))
                wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "table")))

            total_pages = 1
            try:
                page_info_el = wait.until(EC.presence_of_element_located((By.ID, "pageInfo")))
                txt = page_info_el.text.strip()  #
                m = re.search(r"of\s+(\d+)", txt, re.IGNORECASE)
                if m:
                    total_pages = int(m.group(1))
            except Exception:
                total_pages = 1

            print(f"Detected pages: {total_pages}")

            for page in range(1, total_pages + 1):
                html = driver.page_source
                docs = parse_species_table_from_rendered_html(html)

                if not docs:
                    print(f"Page {page}: parsed 0 species rows (table not ready?). Retrying once...")
                    time.sleep(2)
                    html = driver.page_source
                    docs = parse_species_table_from_rendered_html(html)

                for doc in docs:
                    col.update_one(
                        {"taxon_code": doc["taxon_code"]},
                        {"$set": doc},
                        upsert=True,
                    )
                    inserted += 1

                print(f"Page {page}/{total_pages}: upserted {len(docs)} rows (running total ops={inserted})")

                if page < total_pages:
                    next_btn = wait.until(
                        EC.element_to_be_clickable((By.XPATH, "//button[normalize-space()='Next']"))
                    )
                    next_btn.click()

                    try:
                        wait.until(
                            lambda d: d.find_element(By.ID, "pageInfo")
                            .text.strip()
                            .lower()
                            .startswith(f"page {page+1}".lower())
                        )
                    except Exception:
                        time.sleep(1)

            print(f"Done. Total upsert operations: {inserted}")
            return 0

        finally:
            try:
                driver.quit()
            except Exception:
                pass

    except (PyMongoError, RuntimeError) as ex:
        print(f"Taxonomy seed failed: {ex}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
