# KI-gestützte Rezept-Pipeline (CSV/XLSX → WordPress/WPRM mit Bildgenerierung)
<!--
Kurztitel: prägnant halten; wenn du ein Logo hast, füge es oberhalb ein.
-->

<!-- BADGES (optional)
[![Python](https://img.shields.io/badge/python-3.10%2B-blue.svg)]()
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)]()
-->

Dieses Projekt importiert Rezepte aus **CSV/XLSX**, normalisiert Felder, ergänzt fehlende **Summary/Notes/Keywords** via LLM (OpenRouter), generiert auf Wunsch **Food-Bilder** (fal.ai) und veröffentlicht alles **idempotent** über die **WordPress-REST-API** als **WPRM-Rezepte**.  
Abbildung: *Course → WP-Categories*, *Cuisine + Keywords → WP-Tags*.

> **Minimaler Input genügt:** `title`, `ingredients`, `directions`.

---

## Inhaltsverzeichnis
- [Features](#features)
- [Architektur & Workflow](#architektur--workflow)
- [Installation](#installation)
- [CLI/Usage](#cliusage)
- [Publishing-Modi](#publishing-modi)
- [Idempotenz & Duplikate](#idempotenz--duplikate)
- [Fehlerbehandlung & Logs](#fehlerbehandlung--logs)
- [Sicherheitshinweise](#sicherheitshinweise)
- [Troubleshooting](#troubleshooting)
- [Lizenz](#lizenz)

---

## Features
- CSV/XLSX-Import, automatische Spalten-Normalisierung  
- LLM-Kurztexte **nur falls Felder fehlen** (Summary/Notes/Keywords)  
- Regelbasierte Kategorie/Subkategorie, Slug-Erzeugung (ASCII, max. 96)  
- Optionale **Bildgenerierung** (fal.ai) + Upload in WP-Medien  
- WordPress/WPRM-Publishing via REST (**Create/Update über Slug**)  
- Optional: automatische **normale WP-Posts** (mit Featured Image)

<!--
Tipp: Falls du Diet/Allergen später ergänzt, liste es hier als "Experimental" auf.
-->

---

## Architektur & Workflow
1. **Input laden & normalisieren** (CSV `;` oder Excel; flexible Spaltennamen).  
2. **Transformieren**: Zutaten/Schritte säubern, Zeiten/Nährwerte parsen, Slug bilden.  
3. **Meta ergänzen (optional)**: LLM füllt Summary/Notes/Keywords nur bei Leere.  
4. ** Bildgenerierung** via fal.ai → (optional) Upload in WP-Medien.  
5. **WPRM via REST**: vorhandene Rezepte per Slug finden und **updaten** oder **neu anlegen**; WPRM-Taxonomien setzen; Featured Media übernehmen.  
6. ** WP-Posts** erzeugen/aktualisieren: `[wprm-recipe id="…"]`, *Course → Categories*, *Cuisine+Keywords → Tags*.

---

## Installation
```bash
# (optional) virtuelles Environment
python -m venv .venv
source .venv/bin/activate    # Windows: .venv\Scripts\activate

pip install -r requirements.txt
# Falls du keine requirements.txt hast:
pip install pandas requests python-slugify openpyxl
```

## CLI/Usage
```bash
# Minimal: transformieren und als CSV exportieren
python enhanced_recipes_pipeline.py --in rezepte.csv --csv-out output.csv

# Excel-Input (erstes Sheet)
python enhanced_recipes_pipeline.py --in rezepte.xlsx --csv-out output.csv

# Excel-Input (Sheet-Name oder Index)
python enhanced_recipes_pipeline.py --in rezepte.xlsx --sheet "Daten" --csv-out output.csv

# Mit Bildgenerierung (fal.ai) und direktem REST-Publishing nach WordPress
python enhanced_recipes_pipeline.py --in rezepte.csv --csv-out output.csv --images --publish --rest

# Nur WPRM-Posts → normale WP-Posts abbilden/aktualisieren (Course→Categories, Cuisine+Keywords→Tags)
python enhanced_recipes_pipeline.py --in rezepte.csv --csv-out output.csv --make-posts
```


