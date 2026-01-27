import argparse
import json
import os
import asyncio
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
    """Loads the gold standard dataset and limits the number of items if requested."""
    if not os.path.exists(EVAL_SET_FILE):
        raise FileNotFoundError(f"❌ Could not find {EVAL_SET_FILE}")
    
    with open(EVAL_SET_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)
    
    if limit > 0:
        return data[:limit]
    return data

def load_prompt():
    """Loads the system instruction from the text file."""
    if not os.path.exists(PROMPT_FILE):
        raise FileNotFoundError(f"❌ Could not find {PROMPT_FILE}")
    
    with open(PROMPT_FILE, "r", encoding="utf-8") as f:
        return f.read()

def calculate_metrics(gold_set: Set[str], pred_set: Set[str]):
    """Calculates True Positives, False Positives, False Negatives."""
    tp = len(gold_set.intersection(pred_set))
    fp = len(pred_set - gold_set)
    fn = len(gold_set - pred_set)
    return tp, fp, fn

async def run_evaluation(model_key: str, limit: int):
    client = genai.Client(api_key=API_KEY)
    model_name = MODELS.get(model_key)
    
    if not model_name:
        raise ValueError(f"Unknown model key: {model_key}. Available: {list(MODELS.keys())}")

    print(f"🚀 Loading Data...")
    gold_data = load_data(limit)
    print(f"   Loaded {len(gold_data)} items.")

    print(f"📜 Loading Prompt...")
    system_instruction = load_prompt()

    # Prepare Input: Extract ONLY the review text strings to send to the LLM
    # The LLM doesn't see the gold tags, obviously.
    input_reviews = [item["review"] for item in gold_data]
    
    print(f"🤖 Sending request to {model_name}...")
    
    # Configure for JSON output
    config = types.GenerateContentConfig(
        response_mime_type="application/json",
        temperature=0.0,
        system_instruction=system_instruction,
    )

    try:
        response = await client.aio.models.generate_content(
            model=model_name,
            contents=json.dumps(input_reviews), # Send list of strings
            config=config,
        )
        
        # Parse Prediction
        predictions = json.loads(response.text)
        
        # --- SCORING ---
        print("📊 Calculating Metrics...")
        
        total_tp = 0
        total_fp = 0
        total_fn = 0
        
        # Create a map for easy lookup of predictions by review text or index
        # We assume the LLM returns the list in the same order as input
        # Defensive coding: map by index if provided, or assume sequential order
        
        for i, gold_item in enumerate(gold_data):
            gold_pros = set(gold_item.get("pros", []))
            gold_cons = set(gold_item.get("cons", []))
            
            # Find matching prediction (Assuming sequential order from LLM)
            if i < len(predictions):
                pred_item = predictions[i]
                pred_pros = set(pred_item.get("pros", []))
                pred_cons = set(pred_item.get("cons", []))
            else:
                print(f"⚠️ Warning: LLM returned fewer items than input. Missing index {i}")
                pred_pros, pred_cons = set(), set()

            # Pros Metrics
            tp, fp, fn = calculate_metrics(gold_pros, pred_pros)
            total_tp += tp
            total_fp += fp
            total_fn += fn

            # Cons Metrics
            tp, fp, fn = calculate_metrics(gold_cons, pred_cons)
            total_tp += tp
            total_fp += fp
            total_fn += fn

        # Final Calculations
        precision = total_tp / (total_tp + total_fp) if (total_tp + total_fp) > 0 else 0
        recall = total_tp / (total_tp + total_fn) if (total_tp + total_fn) > 0 else 0
        f1 = 2 * (precision * recall) / (precision + recall) if (precision + recall) > 0 else 0

        print("\n" + "="*40)
        print(f"EVAL REPORT: {model_name}")
        print("="*40)
        print(f"Samples Evaluated: {len(gold_data)}")
        print(f"Precision: {precision:.2%}")
        print(f"Recall:    {recall:.2%}")
        print(f"F1 Score:  {f1:.2%}")
        print("-" * 40)
        print(f"Raw Counts -> TP: {total_tp}, FP: {total_fp}, FN: {total_fn}")
        print("="*40)

    except Exception as e:
        print(f"❌ Error during execution: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Eval LLM Classifier")
    parser.add_argument("--limit", type=int, default=10, help="Number of reviews to evaluate (0 for all)")
    parser.add_argument("--model", type=str, choices=["flash", "lite"], default="lite", help="Which model to use")
    
    args = parser.parse_args()
    
    asyncio.run(run_evaluation(args.model, args.limit))