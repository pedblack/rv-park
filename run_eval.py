import argparse
import json
import os
import asyncio
import re
import sys
from typing import List, Dict, Set
from google import genai
from google.genai import types

# --- CONFIGURATION ---
API_KEY = os.environ.get("GOOGLE_API_KEY")
EVAL_SET_FILE = "eval_set.json"
PROMPT_FILE = "llm_prompt.txt"

# Model Options
MODELS = {
    "flash": "gemini-2.5-flash",
    "lite": "gemini-2.5-flash-lite"
}

def load_data(limit: int = 0):
    if not os.path.exists(EVAL_SET_FILE):
        print(f"❌ Critical: {EVAL_SET_FILE} not found.")
        sys.exit(1)
    with open(EVAL_SET_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)
    if limit > 0:
        return data[:limit]
    return data

def load_prompt():
    if not os.path.exists(PROMPT_FILE):
        print(f"❌ Critical: {PROMPT_FILE} not found.")
        sys.exit(1)
    with open(PROMPT_FILE, "r", encoding="utf-8") as f:
        content = f.read()
        # Handle taxonomy injection if present
        if "{pro_taxonomy_block}" in content:
            if not os.path.exists("taxonomy.json"):
                 print("❌ Critical: taxonomy.json needed for prompt but not found.")
                 sys.exit(1)
            with open("taxonomy.json", "r") as tf:
                tax = json.load(tf)
                pro_list = [f"- {item['topic']}: {item['description']}" for item in tax.get("pros", [])]
                con_list = [f"- {item['topic']}: {item['description']}" for item in tax.get("cons", [])]
                content = content.replace("{pro_taxonomy_block}", "\n".join(pro_list))
                content = content.replace("{con_taxonomy_block}", "\n".join(con_list))
        return content

def calculate_metrics(gold_set: Set[str], pred_set: Set[str]):
    tp = len(gold_set.intersection(pred_set))
    fp_set = pred_set - gold_set
    fn_set = gold_set - pred_set
    return len(gold_set.intersection(pred_set)), len(fp_set), len(fn_set), fp_set, fn_set

def extract_json_content(text):
    text = re.sub(r"```json\s*", "", text, flags=re.IGNORECASE)
    text = re.sub(r"```\s*", "", text)
    return text.strip()

async def process_batch(client, model_name, system_instruction, batch_reviews, start_index):
    config = types.GenerateContentConfig(
        response_mime_type="application/json",
        temperature=0.0,
        system_instruction=system_instruction,
    )

    try:
        response = await client.aio.models.generate_content(
            model=model_name,
            contents=f"ANALYZE REVIEWS:\n{json.dumps(batch_reviews)}",
            config=config,
        )
        
        raw_text = extract_json_content(response.text)
        
        try:
            parsed = json.loads(raw_text)
        except json.JSONDecodeError:
            print(f"   ⚠️ JSON Decode Error in batch {start_index}. Response preview: {raw_text[:100]}...")
            return None 

        if isinstance(parsed, list):
            return parsed
        
        if isinstance(parsed, dict):
            for key in ["reviews", "data", "results", "output", "items", "analysis"]:
                if key in parsed and isinstance(parsed[key], list):
                    return parsed[key]
            
            if len(parsed) == 1:
                key = list(parsed.keys())[0]
                if isinstance(parsed[key], list):
                    return parsed[key]

            print(f"   ⚠️ Parsed JSON is a dict but couldn't find the list. Keys: {list(parsed.keys())}")
            return None

        return None

    except Exception as e:
        print(f"   ⚠️ API/Network Error in batch {start_index}: {str(e)}")
        return None

async def run_evaluation(model_key: str, limit: int, batch_size: int):
    client = genai.Client(api_key=API_KEY)
    model_name = MODELS.get(model_key, model_key)
    
    print(f"🚀 Loading Data...")
    gold_data = load_data(limit)
    total_items = len(gold_data)
    print(f"   Loaded {total_items} items to evaluate.")

    print(f"📜 Loading Prompt...")
    system_instruction = load_prompt()

    predictions = []
    errors_occurred = False
    
    effective_batch_size = total_items if batch_size <= 0 else batch_size
    
    print(f"🤖 Sending requests to {model_name}...")
    if batch_size <= 0:
        print(f"   Mode: SINGLE CALL (Batch size: {total_items})")
    else:
        print(f"   Mode: BATCHED (Batch size: {effective_batch_size})")

    for i in range(0, total_items, effective_batch_size):
        batch_gold = gold_data[i : i + effective_batch_size]
        batch_reviews = [item["review"] for item in batch_gold]
        
        current_batch_num = (i // effective_batch_size) + 1
        print(f"   Processing batch {current_batch_num} (Items {i} to {i+len(batch_reviews)})...")
        
        batch_preds = await process_batch(client, model_name, system_instruction, batch_reviews, i)
        
        if batch_preds is not None:
            predictions.extend(batch_preds)
        else:
            print(f"   ❌ Batch {current_batch_num} FAILED.")
            errors_occurred = True 
            predictions.extend([{} for _ in batch_reviews])

    # --- SCORING & DIFF LOGGING ---
    print("\n📊 Calculating Metrics & Diffing...")
    
    total_tp, total_fp, total_fn = 0, 0, 0
    diff_log = []

    for i, gold_item in enumerate(gold_data):
        gold_pros = set(gold_item.get("pros", []))
        gold_cons = set(gold_item.get("cons", []))
        
        if i < len(predictions):
            pred_item = predictions[i]
            if isinstance(pred_item, dict):
                pred_pros = set(pred_item.get("pros", []))
                pred_cons = set(pred_item.get("cons", []))
            else:
                pred_pros, pred_cons = set(), set()
        else:
            pred_pros, pred_cons = set(), set()

        gold_all = gold_pros.union(gold_cons)
        pred_all = pred_pros.union(pred_cons)

        tp, fp, fn, fp_set, fn_set = calculate_metrics(gold_all, pred_all)
        total_tp += tp
        total_fp += fp
        total_fn += fn

        # Log diffs if there are errors
        if fp > 0 or fn > 0:
            review_snippet = gold_item.get('review', '')[:80].replace("\n", " ") + "..."
            diff_entry = {
                "id": i,
                "review": review_snippet,
                "hallucinations (+)": list(fp_set),
                "missed (-)": list(fn_set)
            }
            diff_log.append(diff_entry)

    precision = total_tp / (total_tp + total_fp) if (total_tp + total_fp) > 0 else 0
    recall = total_tp / (total_tp + total_fn) if (total_tp + total_fn) > 0 else 0
    f1 = 2 * (precision * recall) / (precision + recall) if (precision + recall) > 0 else 0

    print("\n" + "="*60)
    print(f"🛑 ERROR ANALYSIS (Showing {len(diff_log)} items)")
    print("="*60)
    
    for diff in diff_log:
        print(f"Review #{diff['id']}: \"{diff['review']}\"")
        if diff["hallucinations (+)"]:
            print(f"   (+) EXTRA: {', '.join(diff['hallucinations (+)'])}")
        if diff["missed (-)"]:
            print(f"   (-) MISSING: {', '.join(diff['missed (-)'])}")
        print("-" * 40)

    print("\n" + "="*40)
    print(f"EVAL REPORT: {model_name}")
    print("="*40)
    print(f"Samples Evaluated: {total_items}")
    print("-" * 40)
    print(f"Precision: {precision:.2%}")
    print(f"Recall:    {recall:.2%}")
    print(f"F1 Score:  {f1:.2%}")
    print("-" * 40)
    print(f"Raw Counts -> TP: {total_tp}, FP: {total_fp}, FN: {total_fn}")
    print("="*40)

    if errors_occurred:
        print("\n❌ FAILED: One or more batches encountered API/Parsing errors.")
        sys.exit(1)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=10, help="Number of reviews to evaluate (0 for all)")
    parser.add_argument("--batch_size", type=int, default=10, help="Items per API call (0 for single call)")
    parser.add_argument("--model", type=str, choices=["flash", "lite"], default="lite")
    args = parser.parse_args()
    
    asyncio.run(run_evaluation(args.model, args.limit, args.batch_size))
