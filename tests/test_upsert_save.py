import os
import sys

os.environ.setdefault("GOOGLE_API_KEY", "test")
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import pandas as pd
import pytest

from backbone_crawler import P4NScraper


def make_row(pid, ts, title=None):
    return {
        "p4n_id": str(pid),
        "title": title or f"place-{pid}",
        "last_scraped": ts,
    }


def test_upsert_and_save_normal(tmp_path):
    out = tmp_path / "out.csv"

    # existing CSV with one row
    existing = pd.DataFrame([make_row(100, "2026-01-01 00:00:00")])
    existing.to_csv(out, index=False)

    scraper = P4NScraper(is_dev=True)
    # point scraper at our test file and inject existing_df
    scraper.csv_file = str(out)
    scraper.existing_df = pd.read_csv(scraper.csv_file)

    # processed batch contains a new row
    scraper.processed_batch = [make_row(200, "2026-01-22 00:00:00")]

    # run save
    scraper._upsert_and_save()

    df = pd.read_csv(out)
    ids = set(df["p4n_id"].astype(str).tolist())
    assert "100" in ids and "200" in ids


def test_upsert_fallback_on_concat_error(tmp_path, monkeypatch):
    out = tmp_path / "out2.csv"

    # start without existing CSV
    scraper = P4NScraper(is_dev=True)
    scraper.csv_file = str(out)
    scraper.existing_df = pd.DataFrame()

    scraper.processed_batch = [make_row(300, "2026-01-22 01:00:00")]

    # force pd.concat to raise so main path fails and fallback append is used
    def fake_concat(*args, **kwargs):
        raise RuntimeError("forced concat error")

    monkeypatch.setattr(pd, "concat", fake_concat)

    # call save; should not raise
    scraper._upsert_and_save()

    # fallback should have created the file with our row
    assert out.exists()
    df = pd.read_csv(out)
    assert "300" in df["p4n_id"].astype(str).tolist()


def test_dedupe_prioritizes_new(tmp_path):
    out = tmp_path / "out3.csv"
    # existing row older
    existing = pd.DataFrame(
        [{"p4n_id": "500", "title": "old-title", "last_scraped": "2025-12-31 00:00:00"}]
    )
    existing.to_csv(out, index=False)

    scraper = P4NScraper(is_dev=True)
    scraper.csv_file = str(out)
    scraper.existing_df = pd.read_csv(scraper.csv_file)

    # new row same id but newer timestamp and different title
    scraper.processed_batch = [
        {"p4n_id": "500", "title": "new-title", "last_scraped": "2026-01-22 12:00:00"}
    ]

    scraper._upsert_and_save()

    df = pd.read_csv(out)
    assert df.shape[0] == 1
    assert df.loc[0, "title"] == "new-title"


def test_p4n_id_type_coercion(tmp_path):
    out = tmp_path / "out4.csv"
    # existing has p4n_id as int in CSV
    existing = pd.DataFrame(
        [{"p4n_id": 600, "title": "int-id", "last_scraped": "2025-12-01 00:00:00"}]
    )
    existing.to_csv(out, index=False)
    scraper = P4NScraper(is_dev=True)
    scraper.csv_file = str(out)
    scraper.existing_df = pd.read_csv(scraper.csv_file)
    # new has same id as string
    scraper.processed_batch = [
        {
            "p4n_id": "600",
            "title": "string-id-new",
            "last_scraped": "2026-01-22 13:00:00",
        }
    ]
    scraper._upsert_and_save()
    df = pd.read_csv(out)
    ids = set(df["p4n_id"].astype(str).tolist())
    assert "600" in ids
    # ensure only one row and title is new
    assert df.shape[0] == 1
    assert df.loc[0, "title"] == "string-id-new"
