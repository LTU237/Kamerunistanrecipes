# ===================== Keys from ENV (safer) =====================
import os, re, io, json, base64, argparse, time, math
from typing import List, Dict, Optional, Tuple

import requests
import pandas as pd
from slugify import slugify

OPENROUTER_API_KEY="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"
# fal.ai (Bilder)
FAL_KEY="d2600f8a-3926-46e7-96eb-4f6638b67b22:0d36ec82a248b9814e465bcc46116936"      # oder FAL_API_KEY

# WordPress (nur für --publish/Bildupload)
WP_URL = "https://kamerunistanrecipes.com"
WP_USER = "admin"
WP_APP_PASSWORD = "Xj2F Vbpg tCVC z1ot 1iPe qXbQ"   # MUSS gesetzt sein
WP_POST_STATUS = "publish"   # "publish" oder "draft"

# ==============================
# ======= CONFIG – EDIT ========
# ==============================
CONFIG = {
    "SITE_LANGUAGE": "en",

    # OpenRouter
    "OPENROUTER_MODEL": "openai/gpt-4o-mini",
    "OPENROUTER_BASE_URL": "https://openrouter.ai/api/v1/chat/completions",
    "MAX_TEXT_TOKENS": 240,
    "TEXT_TEMPERATURE": 0.6,

    # fal.ai
    "FAL_AUTH_SCHEME": "Key",
    "FAL_API_URL": "https://fal.run/fal-ai/flux/schnell",
    "FAL_IMAGE_SIZE": "landscape_4_3",   # kleines "x" verwenden

    # WordPress
    "WP_DEFAULT_STATUS": "publish",
    "WP_UPLOAD_IMAGES": True,

    # Kategorien (Posts spiegeln Course→Categories)
    "CATEGORIES": [
        "Breakfast & Brunch","Beef","Seafood/Fish","Quick & Easy","Low-Fat",
        "Vegetable/Salads","Kid-Friendly","Meatless Meals","Snacks & Appetizers",
        "Drinks & Punches","Holiday Favorites","Sauces & Dips","Poultry Recipes",
        "Breads & Rolls","Soups & Stews","Pasta Dishes","Rice Dishes","desserts",
        "General",
    ],
    "DESSERT_SUBCATEGORIES": ["Pies & Tarts","Cookies & Bars","Muffins & Scones","Cakes"],
    "CATEGORY_RULES": [
        {"match":["pie","tart"], "category":"desserts","subcategory":"Pies & Tarts"},
        {"match":["cookie","cookies","bar","brownie","blondie"], "category":"desserts","subcategory":"Cookies & Bars"},
        {"match":["muffin","scone"], "category":"desserts","subcategory":"Muffins & Scones"},
        {"match":["cake","cupcake","cheesecake"], "category":"desserts","subcategory":"Cakes"},
        {"match":["dessert","pudding","custard","sweet"], "category":"desserts"},
        {"match":["breakfast","brunch","pancake","waffle","omelet","oatmeal","granola","frittata"], "category":"Breakfast & Brunch"},
        {"match":["beef","steak","ground beef","minced beef","brisket"], "category":"Beef"},
        {"match":["fish","salmon","tuna","cod","tilapia","shrimp","prawn","crab","lobster","sardine"], "category":"Seafood/Fish"},
        {"match":["chicken","turkey","duck"], "category":"Poultry Recipes"},
        {"match":["soup","stew","chili","broth"], "category":"Soups & Stews"},
        {"match":["pasta","spaghetti","macaroni","lasagna","noodle"], "category":"Pasta Dishes"},
        {"match":["rice","risotto","pilaf","jollof","fried rice"], "category":"Rice Dishes"},
        {"match":["bread","roll","bun","baguette","flatbread","chapati","biscuit"], "category":"Breads & Rolls"},
        {"match":["salad","coleslaw","slaw","greens","vegetable"], "category":"Vegetable/Salads"},
        {"match":["kid","kids","kid-friendly","toddler","child"], "category":"Kid-Friendly"},
        {"match":["vegetarian","vegan","meatless","plant-based","no meat"], "category":"Meatless Meals"},
        {"match":["snack","appetizer","starter","finger food","tapas"], "category":"Snacks & Appetizers"},
        {"match":["drink","juice","smoothie","shake","punch","lemonade","cocktail","mocktail"], "category":"Drinks & Punches"},
        {"match":["christmas","easter","thanksgiving","holiday","new year","valentine","ramadan","eid"], "category":"Holiday Favorites"},
        {"match":["sauce","dip","dressing","aioli","marinade","gravy","salsa"], "category":"Sauces & Dips"},
        {"match":["low fat","low-fat","lean","light"], "category":"Low-Fat"},
        {"match":["quick","minute","no-cook","weeknight"], "category":"Quick & Easy"},
    ],
    "QUICK_THRESHOLD_MIN": 30,
    "CSV_INCLUDE_SUBCATEGORY": True,
    "REQUEST_TIMEOUT": 60,
    "MAX_FAILS_BEFORE_SKIP": 3,
}
# ==============================
# ===== END CONFIG – EDIT ======
# ==============================

# ---------- sichere String-Helper ----------
# Wandelt einen Wert sicher in String um und ersetzt None/NaN durch ''.
def s(val) -> str:
    """Safely to string; None/NaN -> ''."""
    if val is None:
        return ""
    if isinstance(val, float) and pd.isna(val):
        return ""
    return str(val)

# Gibt getrimmten sicheren String zurück.
def sstrip(val) -> str:
    return s(val).strip()

# ----------------- utils -----------------
# Vereinheitlicht Spaltennamen (klein, alphanumerisch, mit Unterstrichen).
def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    # normalize headers: lower + replace non-alphanum with underscore + trim underscores
    df = df.rename(columns=lambda c: re.sub(r"[^0-9a-zA-Z]+", "_", str(c).strip().lower()).strip("_"))
    return df

# Holt den ersten vorhandenen Spaltenwert aus einer Liste möglicher Namen.
def get_col(row: Dict, *names: str, default: str = "") -> str:
    for n in names:
        if n in row and sstrip(row[n]):
            return sstrip(row[n])
    return default

# Teilt einen Zellenstring in eine Liste anhand von Kommas oder Pipes.
def split_list(cell: Optional[str]) -> List[str]:
    if cell is None or (isinstance(cell, float) and pd.isna(cell)):
        return []
    parts = re.split(r"\s*\|\s*|\s*,\s*", str(cell).strip())
    return [p for p in (x.strip() for x in parts) if p]

# Fügt mehrere Textzeilen mit Zeilenumbrüchen zusammen.
def join_lines(lines: List[str]) -> str:
    return "\n".join([l.strip() for l in lines if l and l.strip()])

# Erkennt Dateiendung (jpg/png/webp) anhand der Bytes.
def _guess_ext(img_bytes: bytes) -> str:
    if not img_bytes:
        return "png"
    if img_bytes.startswith(b"\xff\xd8"):
        return "jpg"
    if img_bytes.startswith(b"\x89PNG\r\n\x1a\n"):
        return "png"
    if img_bytes[:4] == b"RIFF" and img_bytes[8:12] == b"WEBP":
        return "webp"
    return "png"

# ----------------- CATEGORY -----------------
# Wählt Kategorie/Subkategorie basierend auf Schlüsselwörtern oder Zeit.
def choose_category(course: str, title: str, ingredients_text: str, total_minutes: Optional[int]) -> Tuple[str, Optional[str]]:
    hay = f"{course} {title} {ingredients_text}".lower()
    for rule in CONFIG["CATEGORY_RULES"]:
        if any(k.lower() in hay for k in rule.get("match", [])):
            return rule["category"], rule.get("subcategory")
    if total_minutes is not None and total_minutes <= CONFIG["QUICK_THRESHOLD_MIN"]:
        return "Quick & Easy", None
    return "General", None

# --------------- OpenRouter ---------------
# Erzeugt kurze Metadaten (Summary, Serving Ideas, Keywords) via OpenRouter LLM.
def llm_short_meta(title: str, ingredients_text: str, directions_text: str, cuisine: str) -> Dict[str,str]:
    api_key = OPENROUTER_API_KEY
    if not api_key:
        print("[LLM] OPENROUTER_API_KEY fehlt – überspringe Kurzmeta.")
        return {"summary":"", "serving_ideas":"", "keywords":""}

    url = CONFIG["OPENROUTER_BASE_URL"]
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "HTTP-Referer": "https://kamerunistanrecipes.com",
        "X-Title": "Kamerunistan Importer",
    }
    system = "You are a concise English food editor. Be realistic and brief."
    user = f"""
    Write ultra-short meta for a recipe. Max 3 lines total.
    1) A one-line teaser (<=160 chars).
    2) One short serving idea.
    3) Another short serving idea.
    Also output 4-6 SEO keywords (comma separated).
    Title: {title}
    Cuisine: {cuisine}
    Ingredients (top): {ingredients_text.splitlines()[:8]}
    Steps (very short snippet): {directions_text[:300]}
    Reply ONLY valid JSON:
    {{
      "summary":"...",
      "serving_ideas":["...","..."],
      "keywords":"word1, word2, word3"
    }}
    """.strip()
    payload = {
        "model": CONFIG["OPENROUTER_MODEL"],
        "messages": [{"role":"system","content":system},{"role":"user","content":user}],
        "temperature": CONFIG["TEXT_TEMPERATURE"],
        "max_tokens": CONFIG["MAX_TEXT_TOKENS"],
    }
    try:
        r = requests.post(url, headers=headers, data=json.dumps(payload), timeout=CONFIG["REQUEST_TIMEOUT"])
        r.raise_for_status()
        txt = r.json()["choices"][0]["message"]["content"].strip()
        data = json.loads(txt)
        ideas = data.get("serving_ideas", [])
        if not isinstance(ideas, list):
            ideas = [str(ideas)]
        return {
            "summary": (data.get("summary") or "").strip(),
            "serving_ideas": " / ".join([i.strip() for i in ideas if i][:2]),
            "keywords": (data.get("keywords") or "").strip(),
        }
    except Exception as e:
        print(f"[LLM] Fehler bei OpenRouter: {e}")
        return {"summary":"", "serving_ideas":"", "keywords":""}

# ----------------- fal.ai -----------------
# Generiert ein Bild über die fal.ai API aus einem Prompt.
def fal_generate_image(prompt: str) -> Optional[bytes]:
    api = CONFIG["FAL_API_URL"]
    key = FAL_KEY
    if not api or not key:
        print("[IMG] FAL_API_URL oder FAL_KEY fehlt – kein Bild.")
        return None
    headers = {"Authorization": f"{CONFIG['FAL_AUTH_SCHEME']} {key}", "Content-Type": "application/json"}
    payload = {"prompt": prompt, "image_size": CONFIG["FAL_IMAGE_SIZE"]}
    try:
        resp = requests.post(api, headers=headers, data=json.dumps(payload), timeout=CONFIG["REQUEST_TIMEOUT"])
        resp.raise_for_status()
        data = resp.json()
        if isinstance(data, dict):
            if "images" in data and isinstance(data["images"], list):
                im0 = data["images"][0]
                if isinstance(im0, dict) and "b64_json" in im0:
                    return base64.b64decode(im0["b64_json"])
                if isinstance(im0, dict) and "url" in im0:
                    return _download(im0["url"])
            if "b64_json" in data:
                return base64.b64decode(data["b64_json"])
            if "url" in data:
                return _download(data["url"])
        print("[IMG] Unerwartetes fal.ai-Response-Format – kein Bild extrahiert.")
    except Exception as e:
        print(f"[IMG] fal.ai Fehler: {e}")
        return None
    return None

# Lädt eine Datei (z. B. Bild) von einer URL herunter.
def _download(url: str) -> Optional[bytes]:
    try:
        r = requests.get(url, timeout=CONFIG["REQUEST_TIMEOUT"])
        r.raise_for_status()
        return r.content
    except Exception as e:
        print(f"[IMG] Download-Fehler: {e}")
        return None

# ----------------- WordPress: Auth/Terms (WPRM) -----------------
# Erstellt HTTP Basic Auth für WordPress-API.
def wp_auth():
    user = WP_USER; app = WP_APP_PASSWORD
    if not user or not app:
        print("[WP] User/App-Password fehlen – kein Upload möglich.")
        return None
    return requests.auth.HTTPBasicAuth(user, app)

# Holt alle Terms einer WP-Taxonomie (z. B. Kategorien, Tags).
def wp_rest_get_terms(taxonomy: str, search: str = "") -> list:
    url = WP_URL.rstrip("/") + f"/wp-json/wp/v2/{taxonomy}"
    auth = wp_auth()
    if not auth: return []
    out, page = [], 1
    while True:
        params = {"per_page": 100, "page": page}
        if search: params["search"] = search
        r = requests.get(url, auth=auth, params=params, timeout=60)
        if r.status_code == 400 and "rest_post_invalid_page_number" in r.text:
            break
        r.raise_for_status()
        arr = r.json()
        if not arr: break
        out.extend(arr)
        if len(arr) < 100: break
        page += 1
    return out

# Findet die ID eines WP-Terms anhand seines Namens.
def wp_find_term_id(taxonomy: str, name: str) -> Optional[int]:
    if not name: return None
    for t in wp_rest_get_terms(taxonomy, search=name):
        if str(t.get("name","")).strip().lower() == name.strip().lower():
            return int(t.get("id"))
    return None

# Erstellt einen neuen Term in WordPress.
def wp_create_term(taxonomy: str, name: str, parent: Optional[int] = None) -> Optional[int]:
    url = WP_URL.rstrip("/") + f"/wp-json/wp/v2/{taxonomy}"
    auth = wp_auth()
    if not auth or not name: return None
    payload = {"name": name}
    if parent: payload["parent"] = int(parent)
    try:
        r = requests.post(url, auth=auth, json=payload, timeout=60)
        r.raise_for_status()
        return int(r.json().get("id"))
    except Exception as e:
        print(f"[WP TERMS] CREATE {taxonomy} '{name}' failed: {e}")
        return None

# Gibt die ID eines Terms zurück oder erstellt ihn falls nötig.
def wp_ensure_term_id(taxonomy: str, name: str) -> Optional[int]:
    tid = wp_find_term_id(taxonomy, name)
    if tid: return tid
    return wp_create_term(taxonomy, name)

 # Lädt ein Bild in die WordPress-Mediathek hoch.
def wp_upload_image(filename: str, img_bytes: bytes) -> Optional[Dict]:
    """
    Lädt ein Bild in die WP-Mediathek hoch und gibt das JSON der Media-Ressource zurück
    (enthält u. a. 'id' und 'source_url').
    """
    url = WP_URL
    auth = wp_auth()
    if not url or not auth or not img_bytes:
        print("[WP] Fehlende URL/Auth/Bild – Upload übersprungen.")
        return None

    # Dateiendung/MIME raten
    ext = _guess_ext(img_bytes)
    if not filename.lower().endswith((".png", ".jpg", ".jpeg", ".webp")):
        filename = f"{filename}.{ext}"
    mime = "image/jpeg" if ext == "jpg" else f"image/{ext}"

    headers = {
        "Content-Disposition": f'attachment; filename="{filename}"',
        "Content-Type": mime
    }

    try:
        r = requests.post(
            f"{url.rstrip('/')}/wp-json/wp/v2/media",
            headers=headers,
            auth=auth,
            data=img_bytes,
            timeout=120
        )
        r.raise_for_status()
        return r.json()  # enthält 'id' und 'source_url'
    except Exception as e:
        print(f"[WP] Upload-Fehler: {e}")
        if 'r' in locals():
            try:
                print(f"[WP] Status: {r.status_code}, Antwort: {r.text[:500]}")
            except Exception:
                pass
        return None

# ----------------- WordPress: Standard Categories/Tags (für Posts) -----------------
# Holt WP-Standardterms (categories/tags) via REST.
def wp_rest_get_wp_terms(taxonomy: str, search: str = "") -> list:
    # taxonomy: "categories" oder "tags"
    url = WP_URL.rstrip("/") + f"/wp-json/wp/v2/{taxonomy}"
    auth = wp_auth()
    if not auth: return []
    out, page = [], 1
    while True:
        params = {"per_page": 100, "page": page}
        if search: params["search"] = search
        r = requests.get(url, auth=auth, params=params, timeout=60)
        if r.status_code == 400 and "rest_post_invalid_page_number" in r.text:
            break
        r.raise_for_status()
        arr = r.json()
        if not arr: break
        out.extend(arr)
        if len(arr) < 100: break
        page += 1
    return out

# Findet die ID eines Standard-WP-Terms.
def wp_find_wp_term_id(taxonomy: str, name: str) -> Optional[int]:
    if not name: return None
    for t in wp_rest_get_wp_terms(taxonomy, search=name):
        if str(t.get("name","")).strip().lower() == name.strip().lower():
            return int(t.get("id"))
    return None

# Erstellt neuen WP-Standard-Term
def wp_create_wp_term(taxonomy: str, name: str, parent: Optional[int] = None) -> Optional[int]:
    url = WP_URL.rstrip("/") + f"/wp-json/wp/v2/{taxonomy}"
    auth = wp_auth()
    if not auth or not name:
        return None
    payload = {"name": name}
    if taxonomy == "categories" and parent:
        payload["parent"] = int(parent)
    try:
        r = requests.post(url, auth=auth, json=payload, timeout=60)
        r.raise_for_status()
        return int(r.json().get("id"))
    except requests.HTTPError as e:
        # Falls der Term schon existiert, liefert WP 400 + {"code":"term_exists","data":{"term_id":<id>},...}
        try:
            data = r.json()
            if str(data.get("code")) == "term_exists":
                tid = data.get("data", {}).get("term_id")
                if tid:
                    return int(tid)
        except Exception:
            pass
        print(f"[WP {taxonomy.upper()}] CREATE '{name}' failed: {e}")
        return None
    except Exception as e:
        print(f"[WP {taxonomy.upper()}] CREATE '{name}' failed: {e}")
        return None

# Holt Details eines WP-Terms anhand der ID.
def wp_get_wp_term(taxonomy: str, term_id: int) -> Optional[dict]:
    url = WP_URL.rstrip("/") + f"/wp-json/wp/v2/{taxonomy}/{int(term_id)}"
    auth = wp_auth()
    try:
        r = requests.get(url, auth=auth, timeout=60)
        r.raise_for_status()
        return r.json()
    except Exception:
        return None

# Setzt oder ändert den Parent einer Kategorie.
def wp_update_wp_term_parent(taxonomy: str, term_id: int, parent_id: int) -> Optional[int]:
    """Setzt/ändert den Parent eines Terms (nur 'categories' unterstützt Parent)."""
    url = WP_URL.rstrip("/") + f"/wp-json/wp/v2/{taxonomy}/{int(term_id)}"
    auth = wp_auth()
    payload = {}
    if taxonomy == "categories":
        payload["parent"] = int(parent_id)
    else:
        return term_id  # andere Taxonomien haben keinen Parent
    try:
        r = requests.post(url, auth=auth, json=payload, timeout=60)
        r.raise_for_status()
        return int(r.json().get("id"))
    except Exception as e:
        print(f"[WP {taxonomy.upper()}] UPDATE parent of term {term_id} -> {parent_id} failed: {e}")
        return None

# Sichert, dass ein WP-Standard-Term existiert und gibt seine ID zurück.
def wp_ensure_wp_term_id(taxonomy: str, name: str) -> Optional[int]:
    tid = wp_find_wp_term_id(taxonomy, name)
    if tid: return tid
    return wp_create_wp_term(taxonomy, name)

# Stellt sicher, dass Kategorie und optionale Subkategorie existieren und korrekt verknüpft sind.
def ensure_wp_category_hierarchy(cat_name: str, sub_name: Optional[str]) -> List[int]:
    """
    Stellt sicher:
      - Parent-Kategorie 'cat_name' existiert.
      - (optional) Sub-Kategorie 'sub_name' existiert; falls schon existiert, wird
        ihr Parent auf 'cat_name' gesetzt (wenn abweichend).
    Gibt die IDs in der Reihenfolge [parent_id, child_id?] zurück.
    """
    ids: List[int] = []
    if not cat_name:
        return ids

    parent_id = wp_ensure_wp_term_id("categories", cat_name)
    if not parent_id:
        return ids
    ids.append(parent_id)

    if sub_name:
        # 1) existiert schon?
        child_id = wp_find_wp_term_id("categories", sub_name)
        if child_id:
            # 2) Parent prüfen – wenn falsch, korrigieren
            term = wp_get_wp_term("categories", child_id)
            current_parent = (term or {}).get("parent", 0)
            if int(current_parent or 0) != int(parent_id):
                upd = wp_update_wp_term_parent("categories", child_id, parent_id)
                if upd:
                    child_id = upd
            ids.append(child_id)
        else:
            # 3) neu erstellen (mit Parent)
            created = wp_create_wp_term("categories", sub_name, parent=parent_id)
            if created:
                ids.append(created)

    return ids

# Erstellt oder findet WP-Tags basierend auf Cuisine und Keywords.
def ensure_wp_tags_from_cuisine_keywords(cuisine: str, keywords_csv: str) -> List[int]:
    """
    Baut WP-Tag-IDs aus Cuisine (ein Begriff) + Keywords (comma separated).
    """
    names: List[str] = []
    if cuisine:
        names.append(cuisine.strip())
    if keywords_csv:
        names += [k.strip() for k in keywords_csv.split(",") if k.strip()]
    ids: List[int] = []
    for n in names:
        tid = wp_ensure_wp_term_id("tags", n)
        if tid:
            ids.append(tid)
    return ids

 # Holt Termnamen für eine Liste von IDs.
def wp_get_term_names_by_ids(taxonomy: str, ids: List[int]) -> List[str]:
    """Holt Term-Namen für gegebene IDs eines Taxonomy-Endpoints (z. B. wprm_course)."""
    if not ids: return []
    url = WP_URL.rstrip("/") + f"/wp-json/wp/v2/{taxonomy}"
    auth = wp_auth()
    params = {"include": ",".join(str(i) for i in ids), "per_page": len(ids)}
    try:
        r = requests.get(url, auth=auth, params=params, timeout=60)
        r.raise_for_status()
        arr = r.json()
        return [t.get("name","") for t in arr if t.get("name")]
    except Exception as e:
        print(f"[WP TERMS] Namen holen fehlgeschlagen ({taxonomy}): {e}")
        return []

# ----------------- core transform -----------------
# Erstellt eine kurze Promptbeschreibung für ein Foodbild.
def build_image_prompt(title: str, ingredients_text: str) -> str:
    ing = [x for x in ingredients_text.splitlines() if x.strip()][:5]
    return (
        "Appetizing photoreal food image, natural light, soft shadows, cooked, "
        "as it would be in a finished recipe, overhead or 3:2 crop. "
        f"Dish: {title}. Key ingredients: {', '.join(ing)}"
    )

# Erkennt und formatiert die Zutatenliste aus mehreren möglichen Quellenfeldern.
def detect_ingredients_text(row: Dict) -> str:
    """Prefer rich 'ingredients' (oft mit Mengen), sonst 'ingred', sonst gemerged."""
    candidates = [
        sstrip(row.get("ingredients")),
        sstrip(row.get("ingred")),
        sstrip(row.get("gemerged")),
        sstrip(row.get("merge_truncate")),
    ]
    for c in candidates:
        if c:
            parts = [p for p in re.split(r"\s*,\s*|\s*\|\s*", c) if p]
            return "\n".join(parts)
    return ""

# Holt numerische Felder und bewahrt „0“-Werte.
def getv_keep_zero(row: dict, *names: str) -> str:
    for n in names:
        v = sstrip(row.get(n))
        if v is None:
            continue
        if v == "0" or v == "0.0":
            return "0"
        if v not in ("", "nan"):
            return v
    return ""

# Transformiert eine Rezeptzeile in ein vereinheitlichtes Datenformat mit LLM-Metadaten.
def transform_row(r: Dict) -> Dict:
    # --- Basis ---
    title = get_col(r, "seo_title", "title", "name") or "Untitled Recipe"
    slugv = (get_col(r, "slug") or slugify(title))[:96]

    # --- Zutaten & Schritte ---
    ingredients_text = detect_ingredients_text(r)
    directions_text  = get_col(r, "instructions", "directions", "method") or ""
    steps = [s_.strip() for s_ in re.split(r"\n+|(?<=[.;!?])\s+", directions_text) if s_.strip()]
    instructions_lines = join_lines(steps)

    # --- Taxonomien/Meta ---
    course  = "|".join(split_list(get_col(r, "course", "course_meal_type")))
    cuisine = get_col(r, "cuisine", "cuisine_origin")
    equip   = split_list(get_col(r, "equipments", "equipment"))

    # --- Zeiten (Minuten) ---
    def as_minutes(x):
        try:
            s_ = str(x).strip().replace(",", ".")
            if s_ == "":
                return 0
            return int(round(float(s_)))
        except Exception:
            return 0
    prep_m  = as_minutes(r.get("prep_time_minutes"))
    cook_m  = as_minutes(r.get("cook_time_minutes"))
    total_m = as_minutes(r.get("total_time_minutes"))
    if total_m == 0 and (prep_m or cook_m):
        total_m = prep_m + cook_m

    # --- Nährwerte ---
    calories  = getv_keep_zero(r, "calories", "calories_kcal", "kcal")
    protein_g = getv_keep_zero(r, "protein_g", "protein")
    fat_g     = getv_keep_zero(r, "fat_g", "fat")
    carbs_g   = getv_keep_zero(r, "carbohydrates_g", "carbs_g", "carbohydrate_g", "kohlenhydrate_g", "kohlenhydrate")
    fiber_g   = getv_keep_zero(r, "fiber_g", "fibre_g", "ballaststoffe_g", "ballaststoffe")
    sugar_g   = getv_keep_zero(r, "sugar_g", "sugars_g", "zucker_g", "zucker")
    sodium_mg = getv_keep_zero(r, "sodium_mg", "natrium_mg")
    if sodium_mg == "":
        salt_mg = getv_keep_zero(r, "salt_mg", "salz_mg")
        if salt_mg != "":
            try:
                sodium_mg = str(int(round(float(str(salt_mg).replace(",", ".")) * 0.393)))
            except Exception:
                sodium_mg = "0" if str(salt_mg).strip() in ("0", "0.0") else ""

    # --- LLM-Meta (nur falls leer) ---
    summary  = get_col(r, "summary")
    notes    = get_col(r, "notes")
    keywords = get_col(r, "keywords")
    if not summary or not notes or not keywords:
        meta = llm_short_meta(title, ingredients_text, instructions_lines, cuisine)
        summary  = summary  or meta.get("summary", "").strip()
        notes    = notes    or meta.get("serving_ideas", "").strip()
        keywords = keywords or meta.get("keywords", "").strip()

    cat, sub = choose_category(course, title, ingredients_text, total_m or None)

    return {
        "title": title, "slug": slugv, "summary": summary,
        "ingredients": ingredients_text, "instructions": instructions_lines, "notes": notes,
        "course": course, "cuisine": cuisine, "equipment": join_lines(equip),
        "prep_time_minutes": prep_m,
        "cook_time_minutes": cook_m,
        "total_time_minutes": total_m,
        "calories": calories, "protein_g": protein_g, "fat_g": fat_g,
        "carbohydrates_g": carbs_g, "fiber_g": fiber_g, "sugar_g": sugar_g, "sodium_mg": sodium_mg,
        "keywords": keywords, "language": CONFIG["SITE_LANGUAGE"],
        "source_id": s(r.get("id")) or s(r.get("source_id")),

        # CSV-Felder (fallback)
        "category": cat, "subcategory": sub or "",

        # Medienplatzhalter
        "image_url": "", "image_id": None,
    }

_ING_UNIT_MAP = {
    "g":"g","gram":"g","grams":"g","kg":"kg",
    "ml":"ml","l":"l","liter":"l","litre":"l",
    "tsp":"tsp","teaspoon":"tsp","teaspoons":"tsp","tl":"tsp",
    "tbsp":"tbsp","tablespoon":"tbsp","tablespoons":"tbsp","el":"tbsp",
    "cup":"cup","cups":"cup",
    "oz":"oz","ounce":"oz","ounces":"oz",
    "lb":"lb","pound":"lb","pounds":"lb",
    "pinch":"pinch","clove":"clove","cloves":"clove",
    "piece":"piece","pieces":"piece","pcs":"piece","stück":"piece",
    "can":"can","cans":"can",
}

# Wandelt Bruchzahlen (z. B. 1/2) oder Dezimalzahlen in Float um.
def _parse_fraction(txt: str) -> float:
    txt = txt.replace(",", ".").strip()
    if "/" in txt:
        try:
            a,b = txt.split("/", 1)
            return float(a)/float(b)
        except Exception:
            return math.nan
    try:
        return float(txt)
    except Exception:
        return math.nan

# Zerlegt eine Zutatenzeile in Menge, Einheit und Name.
def parse_ingredient_line(line: str):
    line = (line or "").strip()
    if not line:
        return {"amount":"", "unit":"", "name":""}

    m = re.match(r"^\s*(?P<amt>\d+(?:[.,]\d+)?|\d+\s*/\s*\d+)?\s*(?P<unit>[A-Za-zäöüÄÖÜ]+)?\s*(?P<name>.+?)\s*$", line)
    if not m:
        return {"amount":"", "unit":"", "name":line}
    amt = m.group("amt") or ""
    unit = (m.group("unit") or "").lower()
    name = (m.group("name") or "").strip()

    unit = _ING_UNIT_MAP.get(unit, unit)

    val = _parse_fraction(amt) if amt else math.nan
    amount = "" if (amt=="" or math.isnan(val)) else (int(val) if abs(val-int(val))<1e-9 else val)

    return {"amount": amount, "unit": unit, "name": name}

# Gibt eine Zeitstruktur (Tage, Stunden, Minuten) für WPRM zurück.
def wprm_time_block(total_minutes: int) -> dict:
    try:
        m = int(total_minutes)
        if m < 0:
            m = 0
    except Exception:
        m = 0
    return {
        "time": {
            "days":    m // 1440,
            "hours":  (m % 1440) // 60,
            "minutes": m % 60,
        },
        "text": ""
    }

 # Wandelt Text oder Zahl in ganzzahlige Minuten um.
def _as_int_minutes(x) -> int:
    try:
        s_ = str(x).strip().replace(",", ".")
        if s_ == "":
            return 0
        return int(round(float(s_)))
    except Exception:
        return 0

# Wandelt ein transformiertes Rezept in eine CSV-kompatible Zeile um.
def to_wprm_row(r: Dict) -> Dict:
    row = {
        "title": r.get("title",""),
        "slug": r.get("slug",""),
        "summary": r.get("summary",""),
        "ingredients": r.get("ingredients",""),
        "instructions": r.get("instructions",""),
        "notes": r.get("notes",""),
        "course": r.get("course",""),
        "cuisine": r.get("cuisine",""),
        "equipment": r.get("equipment",""),
        "prep_time_minutes": r.get("prep_time_minutes",""),
        "cook_time_minutes": r.get("cook_time_minutes",""),
        "total_time_minutes": r.get("total_time_minutes",""),
        "calories": r.get("calories",""),
        "protein_g": r.get("protein_g",""),
        "fat_g": r.get("fat_g",""),
        "carbohydrates_g": r.get("carbohydrates_g",""),
        "fiber_g": r.get("fiber_g",""),
        "sugar_g": r.get("sugar_g",""),
        "sodium_mg": r.get("sodium_mg",""),
        "keywords": r.get("keywords",""),
        "language": r.get("language",""),
        "source_id": r.get("source_id",""),
        "image_url": r.get("image_url",""),
        "category": r.get("category",""),
    }
    if CONFIG["CSV_INCLUDE_SUBCATEGORY"]:
        row["subcategory"] = r.get("subcategory", "")
    return row

# Konvertiert ein Rezept in das JSON-Format für den WPRM-REST-Import.
def to_wprm_json_item(r: Dict) -> Dict:
    ing_objs = []
    for line in r.get("ingredients","").splitlines():
        line = line.strip()
        if not line:
            continue
        parsed = parse_ingredient_line(line)
        ing_objs.append({
            "amount": parsed["amount"],
            "unit":   parsed["unit"],
            "name":   parsed["name"],
            "notes":  ""
        })

    instructions = [{"text": line} for line in r.get("instructions","").splitlines() if line.strip()]

    prep_m  = _as_int_minutes(r.get("prep_time_minutes", 0))
    cook_m  = _as_int_minutes(r.get("cook_time_minutes", 0))
    total_m = _as_int_minutes(r.get("total_time_minutes", 0))
    if total_m == 0 and (prep_m or cook_m):
        total_m = prep_m + cook_m

    times = {
        "prep":  wprm_time_block(prep_m),
        "cook":  wprm_time_block(cook_m),
        "total": wprm_time_block(total_m),
        "custom": []
    }

    course_terms  = [t for t in r.get("course","").split("|") if t.strip()]
    cuisine_terms = [r["cuisine"]] if r.get("cuisine") else []
    keyword_terms = [k.strip() for k in (r.get("keywords","") or "").split(",") if k.strip()]

    recipe = {
        "name": r.get("title",""),
        "slug": r.get("slug",""),
        "summary": r.get("summary",""),
        "notes": r.get("notes",""),
        "keywords": ", ".join(keyword_terms),

        "times": times,

        # REST-Minutenfelder
        "prep_time":  prep_m,
        "cook_time":  cook_m,
        "total_time": total_m,

        "servings": {"amount": None, "unit": ""},
        "ingredients": [{"group": "", "ingredients": ing_objs}],
        "instructions": [{"group": "", "instructions": instructions}],
        "equipment": [{"name": e} for e in r.get("equipment","").splitlines() if e.strip()],
        "nutrition": {
            "calories":      str(r.get("calories","")),
            "carbohydrates": str(r.get("carbohydrates_g","")),
            "protein":       str(r.get("protein_g","")),
            "fat":           str(r.get("fat_g","")),
            "fiber":         str(r.get("fiber_g","")),
            "sugar":         str(r.get("sugar_g","")),
            "sodium":        str(r.get("sodium_mg","")),
        },
        "taxonomy": {"course": course_terms, "cuisine": cuisine_terms, "keyword": keyword_terms},
        "terms":    {"wprm_course": course_terms, "wprm_cuisine": cuisine_terms, "wprm_keyword": keyword_terms},
    }

    if r.get("image_id"):
        recipe["image_id"] = r.get("image_id")
    elif r.get("image_url"):
        recipe["image_url"] = r.get("image_url")

    status = os.environ.get("WP_POST_STATUS", CONFIG.get("WP_DEFAULT_STATUS","draft"))
    return {
        "status": status,
        "slug": r.get("slug",""),
        "title": r.get("title",""),
        "recipe": recipe,
        # Namen der Taxonomie-Terms für den nächsten Schritt:
        "taxonomy_names": {
            "wprm_course": course_terms,
            "wprm_cuisine": cuisine_terms,
            "wprm_keyword": keyword_terms,
        },
    }

# Erstellt ein neues Rezept via WPRM REST API in WordPress.
def wprm_create_recipe_via_rest(item: Dict) -> Optional[Dict]:
    url = WP_URL.rstrip("/") + "/wp-json/wp/v2/wprm_recipe"
    auth = wp_auth()
    if not auth:
        print("[WPRM REST] Keine Auth.")
        return None

    payload = {
        "status": item.get("status") or os.environ.get("WP_POST_STATUS", CONFIG.get("WP_DEFAULT_STATUS","draft")),
        "slug": item.get("slug",""),
        "title": item.get("title",""),
        "recipe": item.get("recipe", {}),
    }

    # ► Falls ein Bild im Recipe steckt, auch als Featured Image setzen
    img_id = (item.get("recipe") or {}).get("image_id")
    if img_id:
        payload["featured_media"] = int(img_id)

    names = (item.get("taxonomy_names") or {})
    def _clean(xs): return [x for x in (xs or []) if str(x).strip()]
    courses  = _clean(names.get("wprm_course"))
    cuisines = _clean(names.get("wprm_cuisine"))
    keywords = _clean(names.get("wprm_keyword"))

    course_ids  = [wp_ensure_term_id("wprm_course", n)  for n in courses]
    cuisine_ids = [wp_ensure_term_id("wprm_cuisine", n) for n in cuisines]
    keyword_ids = [wp_ensure_term_id("wprm_keyword", n) for n in keywords]

    course_ids  = [int(i) for i in course_ids  if i]
    cuisine_ids = [int(i) for i in cuisine_ids if i]
    keyword_ids = [int(i) for i in keyword_ids if i]

    if course_ids:  payload["wprm_course"]  = course_ids
    if cuisine_ids: payload["wprm_cuisine"] = cuisine_ids
    if keyword_ids: payload["wprm_keyword"] = keyword_ids

    try:
        r = requests.post(url, auth=auth, json=payload, timeout=120)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"[WPRM REST] Fehler: {e}")
        if 'r' in locals():
            print(f"[WPRM REST] {r.status_code} {r.text[:400]}")
        return None

# Findet bestehende WPRM-Rezepte anhand ihres Slugs.
def wp_get_wprm_by_slug(slug: str) -> list:
    """Findet vorhandene WPRM-Rezepte per Slug (status:any)."""
    url = WP_URL.rstrip("/") + "/wp-json/wp/v2/wprm_recipe"
    auth = wp_auth()
    params = {"slug": slug, "status": "any", "per_page": 1}
    try:
        r = requests.get(url, auth=auth, params=params, timeout=60)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"[WPRM REST] Lookup by slug '{slug}' fehlgeschlagen: {e}")
        return []
    
# Aktualisiert ein bestehendes WPRM-Rezept via REST.
def wprm_update_recipe_via_rest(recipe_id: int, item: Dict) -> Optional[Dict]:
    url = WP_URL.rstrip("/") + f"/wp-json/wp/v2/wprm_recipe/{int(recipe_id)}"
    auth = wp_auth()
    if not auth:
        print("[WPRM REST] Keine Auth (update).")
        return None

    names = (item.get("taxonomy_names") or {})
    def _clean(xs): return [x for x in (xs or []) if str(x).strip()]
    courses  = _clean(names.get("wprm_course"))
    cuisines = _clean(names.get("wprm_cuisine"))
    keywords = _clean(names.get("wprm_keyword"))

    course_ids  = [wp_ensure_term_id("wprm_course", n)  for n in courses]
    cuisine_ids = [wp_ensure_term_id("wprm_cuisine", n) for n in cuisines]
    keyword_ids = [wp_ensure_term_id("wprm_keyword", n) for n in keywords]

    course_ids  = [int(i) for i in course_ids  if i]
    cuisine_ids = [int(i) for i in cuisine_ids if i]
    keyword_ids = [int(i) for i in keyword_ids if i]

    payload = {
        "status": item.get("status") or os.environ.get("WP_POST_STATUS", CONFIG.get("WP_DEFAULT_STATUS","draft")),
        "slug": item.get("slug",""),
        "title": item.get("title",""),
        "recipe": item.get("recipe", {}),
    }

    # Featured Image auch beim Update setzen/aktualisieren
    img_id = (item.get("recipe") or {}).get("image_id")
    if img_id:
        payload["featured_media"] = int(img_id)

    if course_ids:  payload["wprm_course"]  = course_ids
    if cuisine_ids: payload["wprm_cuisine"] = cuisine_ids
    if keyword_ids: payload["wprm_keyword"] = keyword_ids

    try:
        r = requests.post(url, auth=auth, json=payload, timeout=120)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"[WPRM REST] Update-Fehler für id {recipe_id}: {e}")
        if 'r' in locals():
            print(f"[WPRM REST] {r.status_code} {r.text[:400]}")
        return None

# Aktualisiert Kategorien und Tags eines WP-Posts.
def wp_update_post_terms(post_id: int, category_ids: list = None, tag_ids: list = None) -> Optional[dict]:
    """Aktualisiert Kategorien/Tags eines vorhandenen normalen WP-Posts."""
    url = WP_URL.rstrip("/") + f"/wp-json/wp/v2/posts/{int(post_id)}"
    auth = wp_auth()
    payload = {}
    if category_ids:
        payload["categories"] = [int(x) for x in category_ids if x]
    if tag_ids:
        payload["tags"] = [int(x) for x in tag_ids if x]
    try:
        r = requests.post(url, auth=auth, json=payload, timeout=120)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"[WP] Update Post Terms fehlgeschlagen: {e}")
        if 'r' in locals():
            print(f"[WP] {r.status_code} {r.text[:400]}")
        return None
    
# Setzt das Beitragsbild eines WP-Posts.
def wp_set_post_featured_media(post_id: int, media_id: int) -> Optional[dict]:
    """Setzt/aktualisiert das Beitragsbild eines bestehenden Posts."""
    url = WP_URL.rstrip("/") + f"/wp-json/wp/v2/posts/{int(post_id)}"
    auth = wp_auth()
    payload = {"featured_media": int(media_id)}
    try:
        r = requests.post(url, auth=auth, json=payload, timeout=120)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"[WP] Featured Image setzen fehlgeschlagen: {e}")
        if 'r' in locals():
            print(f"[WP] {r.status_code} {r.text[:400]}")
        return None


# --------- WPRM -> normale Posts erzeugen (Course→Categories, Cuisine+Keywords→Tags) ---------
# Holt normale WP-Posts anhand ihres Slugs.
def wp_get_posts_by_slug(slug: str) -> list:
    url = WP_URL.rstrip("/") + "/wp-json/wp/v2/posts"
    auth = wp_auth()
    r = requests.get(url, auth=auth, params={"slug": slug}, timeout=60)
    r.raise_for_status()
    return r.json()

# Listet alle WPRM-Rezepte über mehrere Seiten auf.
def wp_list_wprm_recipes(max_pages: int = 500, per_page: int = 100) -> list:
    url = WP_URL.rstrip("/") + "/wp-json/wp/v2/wprm_recipe"
    auth = wp_auth()
    out = []
    for page in range(1, max_pages + 1):
        params = {
            "per_page": per_page,
            "page": page,
            "status": "any",
            "orderby": "id",
            "order": "desc",
            "_fields": "id,slug,status,title"
        }
        r = requests.get(url, auth=auth, params=params, timeout=60)
        if r.status_code == 400 and "rest_post_invalid_page_number" in r.text:
            break
        r.raise_for_status()
        arr = r.json()
        if not arr:
            break
        out.extend(arr)
        if len(arr) < per_page:
            break
    print(f"[POSTS] WPRM via REST gefunden: {len(out)} (inkl. Drafts)")
    return out

 # Holt Detaildaten eines bestimmten WPRM-Rezepts.
def wp_get_wprm_recipe_detail(recipe_id: int) -> Optional[dict]:
    """Details inkl. WPRM-Term-IDs holen."""
    url = WP_URL.rstrip("/") + f"/wp-json/wp/v2/wprm_recipe/{recipe_id}"
    auth = wp_auth()
    params = {"_fields": "id,slug,title,wprm_course,wprm_cuisine,wprm_keyword"}
    try:
        r = requests.get(url, auth=auth, params=params, timeout=60)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"[POSTS] Detail-Load {recipe_id} fehlgeschlagen: {e}")
        return None

# Erstellt einen normalen WP-Post für ein Rezept.
def wp_create_post_for_recipe(recipe: dict, category_ids: list = None, tag_ids: list = None, featured_media_id: Optional[int] = None) -> Optional[dict]:
    """Erzeugt einen normalen WP-Beitrag, der das Rezept einbettet, inkl. Featured Image."""
    auth = wp_auth()
    if not auth:
        print("[WP] Keine Auth – kann keine Beiträge erstellen.")
        return None
    title = recipe.get("title", {}).get("rendered", "") or recipe.get("name", "") or "Recipe"
    slug  = recipe.get("slug", f"recipe-{recipe.get('id')}")
    rid   = recipe.get("id")
    content = f'[wprm-recipe id="{rid}"]\n\n<!-- generated by importer -->'
    payload = {
        "title": title,
        "slug": slug,
        "status": os.environ.get("WP_POST_STATUS", CONFIG.get("WP_DEFAULT_STATUS", "publish")),
        "content": content,
    }
    if category_ids:
        payload["categories"] = [int(x) for x in category_ids if x]
    if tag_ids:
        payload["tags"] = [int(x) for x in tag_ids if x]
    if featured_media_id:
        payload["featured_media"] = int(featured_media_id)

    url = WP_URL.rstrip("/") + "/wp-json/wp/v2/posts"
    try:
        r = requests.post(url, auth=auth, json=payload, timeout=120)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"[WP] Post-Create-Fehler: {e}")
        if 'r' in locals():
            print(f"[WP] {r.status_code} {r.text[:400]}")
        return None

# Erstellt automatisch WP-Posts für alle vorhandenen WPRM-Rezepte.
def make_posts_for_all_wprm_recipes():
    recipes = wp_list_wprm_recipes()
    print(f"[POSTS] Gefundene WPRM-Rezepte: {len(recipes)}")
    created = 0; skipped = 0
    for rec in recipes:
        slug = rec.get("slug", f"recipe-{rec.get('id')}")
        try:
            existing = wp_get_posts_by_slug(slug)
        except Exception as e:
            print(f"[POSTS] Lookup-Fehler für {slug}: {e}")
            existing = []
        if existing:
            skipped += 1
            print(f"[POSTS] Skip (Post existiert): {slug}")
            continue

        # ► WPRM Terms lesen und auf WP Categories/Tags spiegeln
        detail = wp_get_wprm_recipe_detail(rec.get("id"))
        wp_cat_ids, wp_tag_ids = [], []
        if detail:
            course_ids  = [int(x) for x in (detail.get("wprm_course") or []) if x]
            cuisine_ids = [int(x) for x in (detail.get("wprm_cuisine") or []) if x]
            keyword_ids = [int(x) for x in (detail.get("wprm_keyword") or []) if x]

            course_names  = wp_get_term_names_by_ids("wprm_course", course_ids)
            cuisine_names = wp_get_term_names_by_ids("wprm_cuisine", cuisine_ids)
            keyword_names = wp_get_term_names_by_ids("wprm_keyword", keyword_ids)

            # Categories = nur Course
            for name in course_names:
                tid = wp_ensure_wp_term_id("categories", name)
                if tid: wp_cat_ids.append(tid)

            # Tags = Cuisine + Keywords
            for name in (cuisine_names + keyword_names):
                tid = wp_ensure_wp_term_id("tags", name)
                if tid: wp_tag_ids.append(tid)

        newp = wp_create_post_for_recipe(rec, category_ids=wp_cat_ids, tag_ids=wp_tag_ids)
        if newp and newp.get("id"):
            created += 1
            print(f"[POSTS] OK: {rec.get('title',{}).get('rendered','(no title)')} -> Post {newp['id']}")
        else:
            print(f"[POSTS] Fehlgeschlagen: {slug}")
    print(f"[POSTS] Fertig. Neu erstellt: {created}, übersprungen (bereits vorhanden): {skipped}")

    

# ----------------- CLI -----------------
 # Liest eine CSV-Datei mit Semikolontrennung ein.
def read_semicolon_csv(path: str) -> pd.DataFrame:
    return pd.read_csv(path, sep=';', encoding='cp1252')

# Führt den gesamten Pipeline-Workflow je nach CLI-Optionen aus.
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="input_path", required=True, help="Path to input .xlsx or .csv")
    ap.add_argument("--sheet", default=0, help="Sheet name/index for .xlsx")
    ap.add_argument("--csv-out", dest="csv_out", default=None, help="Write WPRM import CSV here")
    ap.add_argument("--publish", action="store_true", help="Upload images/recipes to WordPress (requires WP creds)")
    ap.add_argument("--images", action="store_true", help="Generate images (use with --publish to upload)")
    ap.add_argument("--rest", action="store_true", help="Create recipes via REST instead of JSON import UI")
    ap.add_argument("--json-out", dest="json_out", default=None, help="Write WPRM JSON wrapper here")
    ap.add_argument("--make-posts", action="store_true", help="Create WP posts for all existing WPRM recipes")
    args = ap.parse_args()

    # read
    if args.input_path.lower().endswith(".xlsx"):
        sheet = args.sheet
        try:
            sheet = int(sheet)
        except Exception:
            pass
        df = pd.read_excel(args.input_path, sheet_name=sheet)
    else:
        df = read_semicolon_csv(args.input_path)

    df = normalize_columns(df)

    # optional: WP-Preflight
    if args.publish and CONFIG["WP_UPLOAD_IMAGES"] and not wp_auth():
        print("[WP] Keine gültige Auth – Bildupload wird übersprungen.")

    out: List[Dict] = []

    # --- PROCESS ROWS ---
    for i, (_, row) in enumerate(df.iterrows()):
        # Transform
        t = transform_row(row.to_dict())

        # Bilder
        if args.images:
            prompt = build_image_prompt(t["title"], t["ingredients"])
            img_bytes = fal_generate_image(prompt)
            if not img_bytes:
                print(f"[IMG] Generation fehlgeschlagen: {t['title']}")
            elif args.publish and CONFIG["WP_UPLOAD_IMAGES"] and wp_auth():
                uploaded = wp_upload_image(slugify(t["title"]), img_bytes)
                if uploaded and "source_url" in uploaded:
                    t["image_url"] = uploaded["source_url"]
                    t["image_id"]  = uploaded.get("id")
                    print(f"[OK] Upload: {t['title']} -> {t['image_url']}")
                else:
                    print(f"[SKIP] Upload-Response ohne source_url: {t['title']}")
            else:
                print(f"[SKIP] Image upload nicht aktiv (--publish fehlt): {t['title']}")

        # REST-Import (WPRM Rezept anlegen + WPRM-Taxonomien setzen)
        # REST-Import (idempotent: erst prüfen, dann create ODER update)
        if args.rest:
            item = to_wprm_json_item(t)

            # 1) Existiert dieses Rezept schon (per Slug)?
            existing_recipes = wp_get_wprm_by_slug(t["slug"])
            if existing_recipes:
                rid = existing_recipes[0].get("id")
                updated = wprm_update_recipe_via_rest(rid, item)
                if updated:
                    print(f"[REST UPDATE] {t['title']} -> id {rid}")
                    recipe_obj_for_post = {"id": rid, "slug": t["slug"], "title": {"rendered": t["title"]}}
                else:
                    # Falls Update scheitert, nicht erneut erstellen – überspringen
                    recipe_obj_for_post = {"id": rid, "slug": t["slug"], "title": {"rendered": t["title"]}}
            else:
                created = wprm_create_recipe_via_rest(item)
                if created:
                    rid = created.get("id")
                    print(f"[REST OK] {t['title']} -> id {rid}")
                    recipe_obj_for_post = created
                else:
                    recipe_obj_for_post = None

            # 2) Normalen WP-Post nur anlegen, wenn er NICHT existiert; sonst Terms updaten
            if args.publish and recipe_obj_for_post and recipe_obj_for_post.get("id"):
                cat_ids = ensure_wp_category_hierarchy(t.get("category",""), t.get("subcategory","") or None)
                tag_ids = ensure_wp_tags_from_cuisine_keywords(t.get("cuisine",""), t.get("keywords",""))
                featured_id = t.get("image_id")  # ← vom Upload oben

                existing_posts = wp_get_posts_by_slug(t["slug"])
                if existing_posts:
                    post_id = existing_posts[0].get("id")
                    wp_update_post_terms(post_id, category_ids=cat_ids, tag_ids=tag_ids)
                    if featured_id:
                        # nur setzen, wenn noch kein Bild oder du immer überschreiben willst
                        curr_feat = existing_posts[0].get("featured_media", 0) or 0
                        if int(curr_feat) == 0 or int(curr_feat) != int(featured_id):
                            wp_set_post_featured_media(post_id, int(featured_id))
                    print(f"[POST UPDATE] {t['title']} -> Post {post_id} (Terms/Featured aktualisiert)")
                else:
                    post = wp_create_post_for_recipe(recipe_obj_for_post, category_ids=cat_ids, tag_ids=tag_ids, featured_media_id=featured_id)
                    if post and post.get("id"):
                        print(f"[POST OK] {t['title']} -> Post {post['id']} (Categories: {t.get('category')} / {t.get('subcategory')})")


        out.append(t)

    # CSV export (für WP All Import o.ä.)
    if args.csv_out:
        out_df = pd.DataFrame([to_wprm_row(x) for x in out])
        out_df.to_csv(args.csv_out, index=False, encoding="utf-8")
        print(f"Wrote {len(out_df)} rows to {args.csv_out}")

    # JSON export (Wrapper-Liste für WPRM Importer/REST)
    if args.json_out:
        items = [to_wprm_json_item(x) for x in out]
        payload = items
        with open(args.json_out, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        print(f"Wrote {len(items)} recipes to {args.json_out}")

    # Optional: nach dem Rezept-Import Blog-Posts erzeugen, inkl. Spiegelung:
    # Course -> WP Categories, Cuisine+Keywords -> WP Tags
    if args.make_posts:
        make_posts_for_all_wprm_recipes()

if __name__ == "__main__":
    main()
