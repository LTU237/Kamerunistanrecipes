#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Afro–Asian Fusion Recipe AI Transformer (OpenRouter, minimal fields)
===================================================================
- Adapts recipes to deliberate African–Asian fusion with an LLM via OpenRouter.
- Updates title, ingredients, directions, focus keyword and (if needed) times.
- English only, metric units (g, ml).
- **No ai_cuisine, no ai_meta_description, no ai_tags** as requested.
- Model is hardcoded below (MODEL_ID).
"""
import argparse, csv, json, os, re, sys, time, unicodedata
from pathlib import Path
from typing import Dict, Any, List
import requests

OPENROUTER_API_URL = "https://openrouter.ai/api/v1/chat/completions"
MODEL_ID = "openai/gpt-4o-mini"

# Wandelt Text in einen URL-tauglichen, kleingeschriebenen ASCII-Slug mit Einzelbindestrichen um.
def slugify(text: str) -> str:
    text = unicodedata.normalize('NFKD', text).encode('ascii', 'ignore').decode('ascii')
    text = re.sub(r'[^a-zA-Z0-9]+', '-', text).strip('-').lower()
    text = re.sub(r'-{2,}', '-', text)
    return text

# Normalisiert Temperaturangaben (ersetzt kodierte °, ergänzt fehlendes °F und bereinigt Leerzeichen).
def normalize_degree(text: str) -> str:
    if not text:
        return text
    text = text.replace('\\u00b0', '°')
    text = re.sub(r'(\d{2,3})\s*°(?![CF])', r'\1 °F', text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text

# Liest eine UTF-8-CSV mit Semikolontrennung und gibt Zeilen samt Feldnamen zurück.
def read_csv_semicolon(path: Path):
    with path.open('r', encoding='utf-8', newline='') as f:
        reader = csv.DictReader(f, delimiter=';')
        rows = list(reader)
        fields = reader.fieldnames or []
    return rows, fields

# Schreibt eine Semikolon-CSV mit vorgegebener Feldreihenfolge und Header.
def write_csv_semicolon(path: Path, rows, fieldnames):
    with path.open('w', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter=';')
        writer.writeheader()
        for r in rows:
            writer.writerow({k: r.get(k, '') for k in fieldnames})

# Hängt ein Objekt als JSON-Zeile an eine Cache-Datei (.jsonl) an.
def append_cache(cache_path: Path, obj: Dict[str, Any]):
    with cache_path.open('a', encoding='utf-8') as f:
        f.write(json.dumps(obj, ensure_ascii=False) + '\n')

# Lädt die Cache-Datei (.jsonl) und baut ein Dictionary von id→Objekt auf.
def load_cache(cache_path: Path):
    data = {}
    if cache_path.exists():
        with cache_path.open('r', encoding='utf-8') as f:
            for line in f:
                if not line.strip(): continue
                o = json.loads(line)
                data[str(o['id'])] = o
    return data

# Extrahiert und parst JSON aus dem Modell-Output (direkt oder via Regex-Fallback).
def extract_json(text: str):
    try:
        return json.loads(text)
    except Exception:
        pass
    m = re.search(r'([\[{].*[\]}])', text, flags=re.S)
    if m:
        return json.loads(m.group(1))
    raise ValueError("Model did not return valid JSON.")

SYSTEM_PROMPT = """You are a culinary editor specialized in African–Asian fusion recipes.
All outputs must be in English and follow the JSON schema exactly.
Adapt the existing recipe into an Afro–Asian fusion version that is practical, safe, and tasty.
Prefer metric units (g, ml). Keep total time close to original unless necessary to change."""


# NOTE: double braces {{ }} in the JSON schema so .format() does not treat them as placeholders
USER_TEMPLATE = """Transform the recipe below into deliberate Afro–Asian fusion.
- You may replace or add up to 5 key ingredients to achieve a balanced fusion (e.g., peanut+sesame, suya+soy/ginger, berbere+miso, jollof+gochujang, tamarind+palm oil).
- Keep ENGLISH only and metric units (g, ml). Avoid the word "Recipe" in the title.
- Directions: numbered, 6–12 concise steps, imperative voice.
- Times: update if your method requires it.
- SEO: provide a short focus keyword (Yoast-style).
- Output ONLY valid JSON matching the schema. No extra text.

INPUT FIELDS (parsed from semicolon CSV):
{flat}

JSON SCHEMA TO RETURN:
{{
  "seo_title": "string",
  "slug": "string",
  "ingredients": ["qty ingredient, metric", "..."],
  "directions": ["step 1 short", "step 2 short", "..."],
  "prep_time_minutes": number,
  "cook_time_minutes": number,
  "total_time_minutes": number,
  "focus_keyword": "string",
  "notes": "string (optional)",
  "changed": true
}}"""

# Erzeugt den User-Prompt aus einer CSV-Zeile anhand des Templates und eines flachen JSON-Blocks.
def build_user_prompt(row: Dict[str, Any]) -> str:
    flat = {
        "id": row.get("id", ""),
        "title": row.get("seo_title", ""),
        "slug": row.get("slug", ""),
        "course": row.get("course", ""),
        "cuisine": row.get("cuisine", ""),
        "prep_time_minutes": row.get("prep_time_minutes", ""),
        "cook_time_minutes": row.get("cook_time_minutes", ""),
        "total_time_minutes": row.get("total_time_minutes", ""),
        "equipments": row.get("equipments", ""),
        "calories": row.get("Calories", ""),
        "difficulty": row.get("difficulty", ""),
        "protein_g": row.get("Protein", ""),
        "fat_g": row.get("Fat", ""),
        "carbs_g": row.get("Carbohydrates_g", ""),
        "fiber_g": row.get("fiber_g", ""),
        "sugar_g": row.get("sugar_g", ""),
        "sodium_mg": row.get("sodium_mg", ""),
        "ingredients": row.get("Ingredients", ""),
        "directions": row.get("directions", ""),
    }
    return USER_TEMPLATE.format(flat=json.dumps(flat, ensure_ascii=False, indent=2))

# Ruft OpenRouter (Chat Completions) mit JSON-Format und Retry/Backoff auf und liefert den Roh-Content zurück.
def call_openrouter(messages, temperature: float, max_tokens: int, retries: int = 3) -> str:
    api_key = "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"
    if not api_key:
        raise RuntimeError("OPENROUTER_API_KEY is not set.")
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "HTTP-Referer": "https://kamerunistanrecipes.com/",
        "X-Title": "Afro-Asian Fusion CSV Transformer (Minimal)"
    }
    payload = {
        "model": MODEL_ID,
        "messages": messages,
        "temperature": temperature,
        "max_tokens": max_tokens,
        "response_format": {"type": "json_object"}
    }
    backoff = 2.0
    last_err = None
    for _ in range(retries):
        try:
            r = requests.post(OPENROUTER_API_URL, headers=headers, json=payload, timeout=60)
            if r.status_code == 200:
                data = r.json()
                return data["choices"][0]["message"]["content"]
            last_err = f"HTTP {r.status_code}: {r.text[:500]}"
        except requests.RequestException as e:
            last_err = str(e)
        time.sleep(backoff)
        backoff = min(30.0, backoff * 1.7)
    raise RuntimeError(f"OpenRouter failed: {last_err}")

# Merged die AI-Antwort in die Originalzeile und leitet fehlenden Slug aus dem AI-Titel ab.
def merge_row(orig: Dict[str, Any], ai: Dict[str, Any]) -> Dict[str, Any]:
    r = dict(orig)
    r["ai_seo_title"] = ai.get("seo_title", "")
    r["ai_slug"] = ai.get("slug", "") or slugify(ai.get("seo_title",""))
    r["ai_ingredients"] = "; ".join(ai.get("ingredients", []))
    r["ai_directions"] = " | ".join(ai.get("directions", []))
    r["ai_focus_keyword"] = ai.get("focus_keyword", "")
    r["ai_prep_time_minutes"] = ai.get("prep_time_minutes", "")
    r["ai_cook_time_minutes"] = ai.get("cook_time_minutes", "")
    r["ai_total_time_minutes"] = ai.get("total_time_minutes", "")
    r["ai_notes"] = ai.get("notes", "")
    r["ai_changed"] = str(ai.get("changed", ""))
    if not r["ai_slug"] and r["ai_seo_title"]:
        r["ai_slug"] = slugify(r["ai_seo_title"])
    return r

# Verarbeitet Zeilen mit Rate-Limit und Cache, ruft das LLM auf, cached Ergebnisse und erzeugt die AI-Felder.
def process_rows(rows, args, cache, cache_path: Path):
    out_rows = []
    processed = 0
    last_call_ts = 0.0
    min_interval = 60.0 / max(1, args.rpm)

    for row in rows:
        row_id = str(row.get("id", ""))
        if args.start_id and row_id < str(args.start_id):
            continue
        if args.limit and processed >= args.limit:
            break

        row["directions"] = normalize_degree(row.get("directions", ""))

        if not args.force and row_id in cache:
            ai = cache[row_id]["ai"]
            out_rows.append(merge_row(row, ai))
            processed += 1
            continue

        now = time.time()
        wait = (last_call_ts + min_interval) - now
        if wait > 0: time.sleep(wait)

        messages = [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": build_user_prompt(row)}
        ]
        try:
            raw = call_openrouter(messages, args.temperature, args.max_tokens, retries=args.retries)
            ai_obj = extract_json(raw)
        except Exception as e:
            ai_obj = {
                "seo_title": f"Afro–Asian Fusion: {row.get('seo_title','').strip()}",
                "slug": slugify("afro-asian " + (row.get('seo_title','') or row.get('slug',''))),
                "ingredients": [row.get('Ingredients','') or ""],
                "directions": [row.get('directions','') or ""],
                "prep_time_minutes": int(row.get('prep_time_minutes') or 0),
                "cook_time_minutes": int(row.get('cook_time_minutes') or 0),
                "total_time_minutes": int(row.get('total_time_minutes') or 0),
                "focus_keyword": "afro asian fusion recipe",
                "notes": f"Fallback due to error: {e}",
                "changed": False
            }

        append_cache(cache_path, {"id": row_id, "ai": ai_obj})
        out_rows.append(merge_row(row, ai_obj))
        processed += 1
        last_call_ts = time.time()

    return out_rows

# Parst CLI-Argumente, lädt CSV/Cache, startet die Verarbeitung und schreibt die Ausgabe-CSV.
def main():
    ap = argparse.ArgumentParser(description="AI Afro–Asian fusion transformer (minimal fields).")
    ap.add_argument("input_csv", help="Input CSV path (; separated, UTF-8)")
    ap.add_argument("output_csv", help="Output CSV path")
    ap.add_argument("--temperature", type=float, default=0.3, help="Sampling temperature (0.0–1.0)")
    ap.add_argument("--max_tokens", type=int, default=800, help="Max tokens in response")
    ap.add_argument("--rpm", type=int, default=30, help="Requests per minute rate limit")
    ap.add_argument("--limit", type=int, default=0, help="Process only first N rows (0=all)")
    ap.add_argument("--start-id", default="", help="Start from id (string compare)")
    ap.add_argument("--force", action="store_true", help="Ignore cache and redo rows")
    ap.add_argument("--retries", type=int, default=3, help="Retry count on API failure")
    args = ap.parse_args()

    inp = Path(args.input_csv)
    outp = Path(args.output_csv)
    cache_path = outp.with_suffix(outp.suffix + ".cache.jsonl")

    rows, fields = read_csv_semicolon(inp)
    add_fields = [
        "ai_seo_title","ai_slug","ai_ingredients","ai_directions",
        "ai_focus_keyword","ai_prep_time_minutes","ai_cook_time_minutes",
        "ai_total_time_minutes","ai_notes","ai_changed"
    ]
    out_fields = list(dict.fromkeys(fields + add_fields))

    cache = load_cache(cache_path) if cache_path.exists() and not args.force else {}

    out_rows = process_rows(rows, args, cache, cache_path)
    write_csv_semicolon(outp, out_rows, out_fields)

    print(f"Processed {len(out_rows)} rows → {outp}")
    print(f"Cache: {cache_path}")
    print(f"Model used: {MODEL_ID}")

if __name__ == "__main__":
    main()
