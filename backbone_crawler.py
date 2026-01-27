import argparse
import asyncio
import json
import os
import random
import re
import time
from datetime import datetime, timedelta

import pandas as pd
from google import genai
from google.genai import types
from playwright.async_api import async_playwright
from playwright_stealth import Stealth

# --- CONFIGURABLE CONSTANTS ---
FLASH_MODEL = "gemini-2.5-flash"
LITE_MODEL = "gemini-2.5-flash-lite"
REVIEW_COUNT_THRESHOLD = 100  # Threshold to switch between Lite and Flash models
PROD_CSV = "backbone_locations.csv"
DEV_CSV = "backbone_locations_dev.csv"
LOG_FILE = "pipeline_execution.log"
CONCURRENCY_LIMIT = 3
MAX_GEMINI_RETRIES = 3
TAXONOMY_FILE = "taxonomy.json"  # Source of truth for tags
LLM_PROMPT_FILE = "llm_prompt.txt"  # File containing the LLM prompt

AI_DELAY = 1.0
STALENESS_DAYS = 30
MIN_REVIEWS_THRESHOLD = 5
DEV_LIMIT = 1
REVIEW_YEARS = 2  # Only count reviews from the last N years

URL_LIST_FILE = "url_list.txt"
STATE_FILE = "queue_state.json"

GEMINI_API_KEY = os.environ.get("GOOGLE_API_KEY")
P4N_USER = os.environ.get("P4N_USERNAME")
P4N_PASS = os.environ.get("P4N_PASSWORD")


def ts_print(msg):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {msg}", flush=True)


def is_review_within_years(date_str, years=REVIEW_YEARS):
    """Check if review date is within the last N years. Date format: YYYY-MM-DD"""
    try:
        review_date = datetime.strptime(date_str, "%Y-%m-%d")
        cutoff_date = datetime.now() - timedelta(days=years * 365)
        return review_date >= cutoff_date
    except:
        return False


class PipelineLogger:
    _initialized = False

    @staticmethod
    def log_event(event_type, data):
        processed_content = {}
        for k, v in data.items():
            if isinstance(v, str) and (
                v.strip().startswith("{") or v.strip().startswith("[")
            ):
                try:
                    processed_content[k] = json.loads(v)
                except:
                    processed_content[k] = v
            else:
                processed_content[k] = v

        log_entry = {
            "timestamp": datetime.now().isoformat(),
            "type": event_type,
            "content": processed_content,
        }
        mode = "w" if not PipelineLogger._initialized else "a"
        if not PipelineLogger._initialized:
            PipelineLogger._initialized = True

        with open(LOG_FILE, mode, encoding="utf-8") as f:
            header = f"\n{'='*30} {event_type} {'='*30}\n"
            f.write(
                header
                + json.dumps(log_entry, indent=4, default=str, ensure_ascii=False)
                + "\n"
            )


class DailyQueueManager:
    @staticmethod
    def get_next_partition(batch_size=1):
        if not os.path.exists(URL_LIST_FILE):
            ts_print(f"❌ ERROR: {URL_LIST_FILE} not found.")
            return [], 0, 0
        with open(URL_LIST_FILE, "r") as f:
            urls = [line.strip() for line in f if line.strip()]

        if not urls:
            return [], 0, 0

        state = {"current_index": 0}
        if os.path.exists(STATE_FILE):
            try:
                with open(STATE_FILE, "r") as f:
                    state = json.load(f)
            except:
                pass

        start_idx = state.get("current_index", 0)
        if start_idx >= len(urls):
            start_idx = 0

        target_urls = []
        # Fetch 'batch_size' URLs, wrapping around if necessary
        for i in range(batch_size):
            curr = (start_idx + i) % len(urls)
            target_urls.append(urls[curr])

        return target_urls, start_idx + 1, len(urls)

    @staticmethod
    def increment_state(batch_size=1):
        if not os.path.exists(URL_LIST_FILE):
            return
        with open(URL_LIST_FILE, "r") as f:
            urls = [line.strip() for line in f if line.strip()]

        if not urls:
            return

        state = {"current_index": 0}
        if os.path.exists(STATE_FILE):
            try:
                with open(STATE_FILE, "r") as f:
                    state = json.load(f)
            except:
                pass

        # Advance index by batch_size, wrapping modulo length of list
        state["current_index"] = (state.get("current_index", 0) + batch_size) % len(
            urls
        )

        with open(STATE_FILE, "w") as f:
            json.dump(state, f)


client = genai.Client(api_key=GEMINI_API_KEY)


class P4NScraper:
    def __init__(
        self, is_dev=False, force=False, single_url=None, search_url=None, batch_size=1
    ):
        self.is_dev = is_dev
        self.force = force
        self.single_url = single_url
        self.search_url = search_url
        self.batch_size = batch_size
        self.csv_file = DEV_CSV if is_dev else PROD_CSV
        self.processed_batch = []
        self.existing_df = self._load_existing()
        self.stats = {
            "read": 0,
            "discarded_fresh": 0,
            "discarded_low_feedback": 0,
            "gemini_flash_calls": 0,
            "gemini_lite_calls": 0,
            "gemini_errors": 0,
        }
        self.semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)

    def _load_existing(self):
        if os.path.exists(self.csv_file):
            try:
                df = pd.read_csv(self.csv_file)
                df["last_scraped"] = pd.to_datetime(df["last_scraped"], errors="coerce")
                return df
            except:
                pass
        return pd.DataFrame()

    async def analyze_with_ai(self, raw_data, model_name, url):
        if model_name == FLASH_MODEL:
            self.stats["gemini_flash_calls"] += 1
        else:
            self.stats["gemini_lite_calls"] += 1

        # Load dynamic taxonomy from JSON file with descriptions
        try:
            with open(TAXONOMY_FILE, "r", encoding="utf-8") as f:
                tax_data = json.load(f)

                # Format each entry as "topic: description" for better AI context
                pro_list = [
                    f"- {item['topic']}: {item['description']}"
                    for item in tax_data.get("pros", [])
                    if isinstance(item, dict)
                ]
                con_list = [
                    f"- {item['topic']}: {item['description']}"
                    for item in tax_data.get("cons", [])
                    if isinstance(item, dict)
                ]

                pro_taxonomy_block = "\n".join(pro_list)
                con_taxonomy_block = "\n".join(con_list)

            with open(LLM_PROMPT_FILE, "r", encoding="utf-8") as f:
                system_instruction_template = f.read()

            system_instruction = system_instruction_template.format(
                pro_taxonomy_block=pro_taxonomy_block,
                con_taxonomy_block=con_taxonomy_block,
            )
        except Exception as e:
            ts_print(f"❌ FAILED TO LOAD TAXONOMY OR PROMPT: {e}")
            return {}

        num_reviews = len(raw_data.get("all_reviews", []))
        json_payload = json.dumps(raw_data, default=str, ensure_ascii=False)

        PipelineLogger.log_event(
            "SENT_TO_GEMINI", {"payload": raw_data, "model": model_name}
        )

        config = types.GenerateContentConfig(
            response_mime_type="application/json",
            temperature=0.0,
            system_instruction=system_instruction,
        )

        for attempt in range(MAX_GEMINI_RETRIES):
            try:
                await asyncio.sleep(AI_DELAY * (attempt + 1))

                response = await client.aio.models.generate_content(
                    model=model_name,
                    contents=f"ANALYZE:\n{json_payload}",
                    config=config,
                )

                # Attempt to parse JSON immediately inside the try block
                clean_text = re.sub(r"```json\s*|\s*```", "", response.text).strip()
                ai_json = json.loads(clean_text)

                PipelineLogger.log_event(
                    "GEMINI_ANSWER", {"model": model_name, "response": ai_json}
                )
                return ai_json

            except (json.JSONDecodeError, Exception) as e:
                # Force retry for JSON errors OR transient API errors
                is_json_error = isinstance(e, json.JSONDecodeError)
                err_msg = str(e).lower()
                is_transient = (
                    "503" in err_msg or "overloaded" in err_msg or "deadline" in err_msg
                )

                if (is_json_error or is_transient) and attempt < MAX_GEMINI_RETRIES - 1:
                    ts_print(
                        f"🔄 [RETRY {attempt+1}/{MAX_GEMINI_RETRIES}] {type(e).__name__} for {url}. Retrying..."
                    )
                    continue

                ts_print(f"❌ [GEMINI ERROR] URL: {url} | Error: {e}")
                PipelineLogger.log_event(
                    "GEMINI_ERROR", {"error": str(e), "model": model_name}
                )
                self.stats["gemini_errors"] += 1
                return {}

    async def extract_atomic(self, context, url, current_num, total_num):
        async with self.semaphore:
            if self.is_dev and self.stats["read"] >= DEV_LIMIT:
                return

            ts_print(f"➡️  [{current_num}/{total_num}] Scraping: {url}")

            p_id_guess = url.split("/")[-1]
            PipelineLogger.log_event(
                "START_SCRAPE",
                {
                    "url": url,
                    "p4n_id_guess": p_id_guess,
                    "attempt_index": current_num,
                    "total": total_num,
                },
            )

            page = await context.new_page()
            try:
                await page.goto(url, wait_until="domcontentloaded", timeout=30000)
                await page.wait_for_selector(".place-feedback-average", timeout=10000)

                await asyncio.sleep(5.0)

                stats_container = page.locator(".place-feedback-average")

                # DEFENSIVE FIX: Extract review count safely to avoid NoneType error
                raw_count_text = await stats_container.locator("strong").text_content()
                count_match = re.search(r"(\d+)", raw_count_text)
                actual_feedback_count = int(count_match.group(1)) if count_match else 0

                if actual_feedback_count < MIN_REVIEWS_THRESHOLD:
                    ts_print(
                        f"🗑️  [DISCARD] Low feedback ({actual_feedback_count} reviews) for: {url}"
                    )
                    self.stats["discarded_low_feedback"] += 1
                    return

                p_id = (
                    await page.locator("body").get_attribute("data-place-id")
                    or url.split("/")[-1]
                )
                title = (
                    (await page.locator("h1").first.text_content())
                    .split("\n")[0]
                    .strip()
                )

                lat, lng = 0.0, 0.0
                coord_link_el = page.locator("a[href*='lat='][href*='lng=']").first
                coord_link = (
                    await coord_link_el.get_attribute("href")
                    if await coord_link_el.count() > 0
                    else None
                )
                if coord_link:
                    m = re.search(
                        r"lat=([-+]?\d*\.\d+|\d+)&lng=([-+]?\d*\.\d+|\d+)", coord_link
                    )
                    if m:
                        lat, lng = float(m.group(1)), float(m.group(2))

                review_articles = await page.locator(".place-feedback-article").all()
                formatted_reviews, review_seasonality = [], {}

                for article in review_articles:
                    try:
                        date_text = await article.locator(
                            "span.caption.text-gray"
                        ).text_content()
                        text_val = await article.locator(
                            ".place-feedback-article-content"
                        ).text_content()
                        date_parts = date_text.strip().split("/")
                        if len(date_parts) == 3:
                            date_val = (
                                f"{date_parts[2]}-{date_parts[1]}-{date_parts[0]}"
                            )
                            if is_review_within_years(date_val, REVIEW_YEARS):
                                month_key = f"{date_parts[2]}-{date_parts[1]}"
                                review_seasonality[month_key] = (
                                    review_seasonality.get(month_key, 0) + 1
                                )
                                formatted_reviews.append(
                                    f"[{date_val}]: {text_val.strip()}"
                                )
                    except:
                        continue

                raw_payload = {
                    "places_count": (
                        int(val)
                        if (
                            val := await self._get_dl(page, "Number of places")
                        ).isdigit()
                        else 0
                    ),
                    "parking_cost": await self._get_dl(page, "Parking cost"),
                    "all_reviews": formatted_reviews,
                }

                review_count = len(formatted_reviews)
                selected_model = (
                    FLASH_MODEL if review_count > REVIEW_COUNT_THRESHOLD else LITE_MODEL
                )

                # Pass 'url' to enable immediate diagnostic logging on error
                ai_data = await self.analyze_with_ai(raw_payload, selected_model, url)
                top_langs = ai_data.get("top_languages", [])
                pros_cons = ai_data.get("pros_cons") or {}

                # DEFENSIVE FIX: Extract average rating safely to avoid NoneType error
                rating_text = await stats_container.locator(".text-gray").text_content()
                rating_match = re.search(r"(\d+\.?\d*)", rating_text)
                avg_rating = float(rating_match.group(1)) if rating_match else 0.0

                row = {
                    "p4n_id": p_id,
                    "title": title,
                    "url": url,
                    "latitude": lat,
                    "longitude": lng,
                    "location_type": await self._get_type(page),
                    "num_places": ai_data.get("num_places"),
                    "total_reviews": actual_feedback_count,
                    "avg_rating": avg_rating,
                    "parking_min_eur": ai_data.get("parking_min"),
                    "parking_max_eur": ai_data.get("parking_max"),
                    "electricity_eur": ai_data.get("electricity_eur"),
                    "review_seasonality": json.dumps(review_seasonality),
                    "top_languages": "; ".join(
                        [
                            f"{l.get('lang')} ({l.get('count')})"
                            for l in top_langs
                            if isinstance(l, dict)
                        ]
                    ),
                    "ai_pros": "; ".join(
                        [
                            f"{p.get('topic')} ({p.get('count')})"
                            for p in pros_cons.get("pros", [])
                            if isinstance(p, dict)
                        ]
                    ),
                    "ai_cons": "; ".join(
                        [
                            f"{c.get('topic')} ({c.get('count')})"
                            for c in pros_cons.get("cons", [])
                            if isinstance(c, dict)
                        ]
                    ),
                    "last_scraped": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                }
                PipelineLogger.log_event("STORED_ROW", row)
                self.processed_batch.append(row)
                self.stats["read"] += 1
            except Exception as e:
                ts_print(f"⚠️ Error for {url}: {e}")
            finally:
                await page.close()

    async def _get_type(self, page):
        try:
            return await page.locator(".place-header-access img").get_attribute("title")
        except:
            return "Unknown"

    async def _get_dl(self, page, label):
        try:
            return (
                await page.locator(f"dt:has-text('{label}') + dd").first.text_content()
            ).strip()
        except:
            return "N/A"

    async def start(self):
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context()
            page = await context.new_page()
            await Stealth().apply_stealth_async(page)

            if self.single_url:
                target_urls = [self.single_url]
                current_idx, total_idx = 1, 1
            elif self.search_url:
                target_urls = [self.search_url]
                current_idx, total_idx = 1, 1
            else:
                target_urls, current_idx, total_idx = (
                    DailyQueueManager.get_next_partition(self.batch_size)
                )

            ts_print("=" * 60)
            ts_print(
                f"🔍 [SEARCH PAGE] Scraping {len(target_urls)} items starting from: {target_urls[0] if target_urls else 'N/A'}"
            )
            ts_print(f"📅 [PARTITION] Starting index {current_idx} of {total_idx}")
            ts_print("=" * 60)

            if self.single_url:
                discovered = [self.single_url]
            else:
                discovery_links = []
                for url in target_urls:
                    await page.goto(url, wait_until="domcontentloaded")
                    try:
                        await page.wait_for_selector("a[href*='/place/']", timeout=5000)
                    except:
                        pass
                    links = await page.locator("a[href*='/place/']").all()
                    for link in links:
                        href = await link.get_attribute("href")
                        if href:
                            discovery_links.append(
                                f"https://park4night.com{href}"
                                if href.startswith("/")
                                else href
                            )

                discovered = list(set(discovery_links))

            if self.is_dev:
                ts_print(f"🛠️  [DEV MODE] Seeking {DEV_LIMIT} successful run(s)...")

            tasks = []
            for link in discovered:
                if self.is_dev and self.stats["read"] >= DEV_LIMIT:
                    break

                p_id = link.split("/")[-1]
                is_stale = True
                if (
                    not self.force
                    and not self.existing_df.empty
                    and str(p_id) in self.existing_df["p4n_id"].astype(str).values
                ):
                    last_date = self.existing_df[
                        self.existing_df["p4n_id"].astype(str) == str(p_id)
                    ]["last_scraped"].iloc[0]
                    if pd.notnull(last_date) and (
                        datetime.now() - pd.to_datetime(last_date)
                    ) < timedelta(days=STALENESS_DAYS):
                        is_stale = False

                if is_stale or self.force:
                    if self.is_dev:
                        await self.extract_atomic(
                            context, link, self.stats["read"] + 1, "Seeking..."
                        )
                        if self.stats["read"] >= DEV_LIMIT:
                            break
                    else:
                        tasks.append(
                            self.extract_atomic(
                                context, link, len(tasks) + 1, len(discovered)
                            )
                        )
                else:
                    ts_print(f"⏩  [SKIP] Listing fresh: {link}")
                    self.stats["discarded_fresh"] += 1
            try:
                if tasks:
                    await asyncio.gather(*tasks)
            except Exception as e:
                ts_print(f"⚠️ Unhandled error during scraping: {e}")
                PipelineLogger.log_event("RUN_ERROR", {"error": str(e)})
            finally:
                try:
                    await browser.close()
                except Exception:
                    pass

                try:
                    self._upsert_and_save()
                except Exception as e2:
                    ts_print(f"⚠️ Error saving processed batch: {e2}")
                    PipelineLogger.log_event("SAVE_ERROR", {"error": str(e2)})

            ts_print("=" * 40)
            ts_print("🏁 [RUN SUMMARY]")
            ts_print(f"✅ Items Successfully Processed: {self.stats['read']}")
            ts_print(f"⏩ Items Discarded (Fresh): {self.stats['discarded_fresh']}")
            ts_print(
                f"🗑️  Items Discarded (Low Feedback): {self.stats['discarded_low_feedback']}"
            )
            ts_print(
                f"🤖 Total Gemini Flash Calls: {self.stats.get('gemini_flash_calls', 0)}"
            )
            ts_print(
                f"🤖 Total Gemini Flash-Lite Calls: {self.stats.get('gemini_lite_calls', 0)}"
            )
            ts_print(f"❌ Total Gemini Errors: {self.stats.get('gemini_errors', 0)}")
            ts_print("=" * 40)

            if not self.is_dev and not self.single_url and not self.search_url:
                DailyQueueManager.increment_state(self.batch_size)

    def _upsert_and_save(self):
        if not self.processed_batch:
            return

        try:
            new_df = pd.DataFrame(self.processed_batch)

            if "p4n_id" in new_df.columns:
                new_df["p4n_id"] = (
                    new_df["p4n_id"].astype(str).str.strip().replace("nan", "")
                )
            else:
                new_df["p4n_id"] = ""

            if "last_scraped" in new_df.columns:
                new_df["last_scraped"] = pd.to_datetime(
                    new_df["last_scraped"], errors="coerce"
                )
            else:
                new_df["last_scraped"] = pd.NaT

            new_df = new_df[new_df["p4n_id"].astype(bool)].copy()

            existing = (
                self.existing_df.copy()
                if not self.existing_df.empty
                else pd.DataFrame()
            )

            if not existing.empty:
                if "p4n_id" in existing.columns:
                    existing["p4n_id"] = (
                        existing["p4n_id"].astype(str).str.strip().replace("nan", "")
                    )
                else:
                    existing["p4n_id"] = ""

                if "last_scraped" in existing.columns:
                    existing["last_scraped"] = pd.to_datetime(
                        existing["last_scraped"], errors="coerce"
                    )
                else:
                    existing["last_scraped"] = pd.NaT

                existing = existing[existing["p4n_id"].astype(bool)].copy()

            if not new_df.empty:
                new_df["_is_new"] = True
            if not existing.empty:
                existing["_is_new"] = False

            final_df = pd.concat([new_df, existing], ignore_index=True, sort=False)

            if "last_scraped" in final_df.columns:
                final_df["last_scraped"] = pd.to_datetime(
                    final_df["last_scraped"], errors="coerce"
                )

            if "p4n_id" in final_df.columns:
                final_df = final_df.drop_duplicates(subset=["p4n_id"], keep="first")

            if "_is_new" in final_df.columns:
                final_df = final_df.drop(columns=["_is_new"])

            final_df.to_csv(self.csv_file, index=False)

        except Exception as e:
            PipelineLogger.log_event("UPSERT_SAVE_ERROR", {"error": str(e)})
            ts_print(f"⚠️ Saving error: {e}. Attempting fallback append to CSV.")

            try:
                fallback_df = pd.DataFrame(self.processed_batch)
                if "last_scraped" in fallback_df.columns:
                    fallback_df["last_scraped"] = fallback_df["last_scraped"].astype(
                        str
                    )

                if os.path.exists(self.csv_file):
                    fallback_df.to_csv(
                        self.csv_file, mode="a", index=False, header=False
                    )
                else:
                    fallback_df.to_csv(self.csv_file, index=False)
                ts_print(
                    f"💾 Appended {len(fallback_df)} rows to {self.csv_file} (fallback)."
                )
            except Exception as e2:
                PipelineLogger.log_event("UPSERT_SAVE_FINAL_ERROR", {"error": str(e2)})
                ts_print(f"❌ Failed fallback save: {e2}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dev", action="store_true")
    parser.add_argument("--force", action="store_true")
    parser.add_argument(
        "--url",
        type=str,
        default=None,
        help="Crawl a specific location URL (overrides daily queue)",
    )
    parser.add_argument(
        "--search_url",
        type=str,
        default=None,
        help="Crawl a specific search result URL (overrides daily queue, finds places inside)",
    )
    parser.add_argument(
        "--batch_size",
        type=int,
        default=1,
        help="Number of URLs to process from the queue",
    )
    args = parser.parse_args()

    url_arg = None
    if args.url:
        url_arg = args.url.strip()
        if url_arg.startswith("'") and url_arg.endswith("'"):
            url_arg = url_arg[1:-1]
        if url_arg.startswith('"') and url_arg.endswith('"'):
            url_arg = url_arg[1:-1]

    search_url_arg = None
    if args.search_url:
        search_url_arg = args.search_url.strip()
        if search_url_arg.startswith("'") and search_url_arg.endswith("'"):
            search_url_arg = search_url_arg[1:-1]
        if search_url_arg.startswith('"') and search_url_arg.endswith('"'):
            search_url_arg = search_url_arg[1:-1]

    asyncio.run(
        P4NScraper(
            is_dev=args.dev,
            force=args.force,
            single_url=url_arg,
            search_url=search_url_arg,
            batch_size=args.batch_size,
        ).start()
    )
