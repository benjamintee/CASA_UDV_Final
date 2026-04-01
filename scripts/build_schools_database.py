"""
England Secondary Schools Database Builder 
===========================================
Builds a research-ready dataset of all state-funded secondary schools
in England. Filter to London (or any geography) after building.

Data Sources:
  1. GIAS    - School details, location, type
               MANUAL DOWNLOAD — place CSV in large/ folder, script converts to parquet
               https://get-information-schools.service.gov.uk/Downloads
               → "All establishment fields (CSV)"
               Expected filename: large/edubasealldata20260307.csv

  2. Ofsted  - State-funded school inspections as at 31 August 2025
               Auto-downloaded to large/, cached as data/ofsted.parquet

  3. KS4     - Attainment 8, Progress 8, EBacc (2023/24 — last year with P8)
               Auto-downloaded to large/, cached as data/ks4.parquet

  4. SCAP    - School capacity & utilisation, mainstream + sixth form (2023/24)
               Auto-downloaded to large/, cached as data/scap_mainstream.parquet
                                                       data/scap_sixthform.parquet

  5. IMD     - Index of Multiple Deprivation 2025, File 7
               Auto-downloaded to large/, cached as data/imd.parquet
               Published October 2025. Uses 2021 LSOA boundaries — matches GIAS directly.

  6. Workforce - School Workforce in England — Pupil to teacher ratios, school level
               Reporting Year 2024 (2024/25 academic year)
               Auto-downloaded to large/, cached as data/workforce.parquet

  7. GLA      - Greater London Authority boundary (shapefile)
               Auto-downloaded to large/, cached as data/gla_boundary.parquet
               Used with 5 km buffer to derive top-quartile Att8/P8 flags for
               schools in the London region.

Caching strategy:
  large/   Raw CSVs as downloaded (large files, kept for reference / re-processing)
  data/    Parquet versions (fast to read, type-safe, compressed)

  On each run, ensure_parquet() resolves each source in this order:
    1. data/ parquet exists  → load and return immediately (fast path, no download)
    2. large/ CSV exists     → convert to parquet, save to data/, return
    3. URL available         → download to large/, convert to parquet, save, return
    4. Manual required       → print instructions, return None

  GIAS is manual-only: place the CSV in large/gias_establishments.csv,
  the script converts it and caches it as data/gias.parquet.

Join strategy:
  GIAS (base) → Ofsted → KS4 → SCAP    [all on URN]
  IMD                                   [on lsoa_code — 2021 LSOAs in both GIAS and IMD 2025]

Output:
  england_secondary_schools.parquet
  england_secondary_schools.csv

Post-processing (London filter example):
  df = pd.read_parquet("england_secondary_schools.parquet")
  london = df[df["london_sub_region"].isin(["Inner London", "Outer London"])]

Requirements:
  pip install pandas requests pyarrow geopandas shapely
"""

import os
import io
import zipfile
import pandas as pd
import requests
import geopandas as gpd
from shapely.geometry import Point

# ==============================================================
# DIRECTORIES
# ==============================================================

DIR_LARGE = "large"   # Raw downloaded CSVs
DIR_DATA  = "data"    # Parquet cache (fast reads)

os.makedirs(DIR_LARGE, exist_ok=True)
os.makedirs(DIR_DATA,  exist_ok=True)

# ==============================================================
# SOURCE REGISTRY
# url=None means manual download required (GIAS)
# ==============================================================

SOURCES = {
    "gias": {
        "url":      None,   # Manual download required
        "large":    "large/edubasealldata20260307.csv",
        "data":     "data/gias.parquet",
        "encoding": "latin-1",
        "manual_instructions": (
            "Download from: https://get-information-schools.service.gov.uk/Downloads\n"
            "  → Click 'All establishment fields (CSV)'\n"
            "  → Save as: large/edubasealldata20260307.csv"
        ),
    },
    "ofsted": {
        "url": (
            "https://assets.publishing.service.gov.uk/media/691ee0612a687551bd8153da/"
            "State-funded_schools_inspections_and_outcomes_as_at_31_August_2025.csv"
        ),
        "large":    "large/ofsted_31aug2025.csv",
        "data":     "data/ofsted.parquet",
        "encoding": "latin-1",
    },
    "ks4": {
        "url": (
            "https://explore-education-statistics.service.gov.uk"
            "/data-catalogue/data-set/c8f753ef-b76f-41a3-8949-13382e131054/csv"
        ),
        "large":    "large/ks4_202324_schools_final.csv",
        "data":     "data/ks4.parquet",
        "encoding": "utf-8",
    },
    "scap_mainstream": {
        "url": (
            "https://explore-education-statistics.service.gov.uk"
            "/data-catalogue/data-set/bf165de6-3b9a-4014-83b5-232454343797/csv"
        ),
        "large":    "large/scap_mainstream_200910-202324.csv",
        "data":     "data/scap_mainstream.parquet",
        "encoding": "utf-8",
    },
    "scap_sixthform": {
        "url": (
            "https://explore-education-statistics.service.gov.uk"
            "/data-catalogue/data-set/1dadad6e-8146-43b5-9a45-861eff79e1ad/csv"
        ),
        "large":    "large/scap_sixthform_201718-202324.csv",
        "data":     "data/scap_sixthform.parquet",
        "encoding": "utf-8",
    },
    "imd": {
        "url": (
            "https://assets.publishing.service.gov.uk/media/691ded56d140bbbaa59a2a7d/"
            "File_7_IoD2025_All_Ranks_Scores_Deciles_Population_Denominators.csv"
        ),
        "large":    "large/imd2025_file7.csv",
        "data":     "data/imd.parquet",
        "encoding": "utf-8",
    },
    "workforce": {
        # School Workforce in England — Pupil to teacher ratios, school level (2024/25)
        # https://explore-education-statistics.service.gov.uk/data-catalogue/data-set/12849f4a-1427-4dc2-9f7a-e069087824aa
        "url": (
            "https://explore-education-statistics.service.gov.uk"
            "/data-catalogue/data-set/12849f4a-1427-4dc2-9f7a-e069087824aa/csv"
        ),
        "large":    "large/workforce_pupil_teacher_ratios_school.csv",
        "data":     "data/workforce.parquet",
        "encoding": "utf-8",
    },
    "gla_boundary": {
        # Greater London Authority boundary (shapefile in zip)
        # Used for spatial filtering: top Att8/P8 quartile within GLA + 5 km buffer
        "url": (
            "https://data.london.gov.uk/download/20od9/"
            "114d1137-e339-4b50-b409-124c17f4b59a/gla.zip"
        ),
        "large":    "large/gla.zip",
        "data":     "data/gla_boundary.parquet",
        "format":   "shapefile_zip",   # signals special handling (not a CSV)
    },
}

OUTPUT_PARQUET = "england_secondary_schools.parquet"
OUTPUT_CSV     = "england_secondary_schools.csv"


# ==============================================================
# CACHE LAYER
# ==============================================================

def ensure_parquet(key):
    """
    Guarantee data/{key}.parquet exists and return it as a DataFrame.

    Resolution order:
      1. data/ parquet exists → load and return (fast path, no network call)
      2. large/ CSV exists    → convert to parquet, save to data/, return
      3. URL available        → download to large/, convert to parquet, return
      4. Manual required      → print instructions, return None

    Args:
      key: str — key in SOURCES dict

    Returns:
      pd.DataFrame or None if source is unavailable
    """
    src      = SOURCES[key]
    parquet  = src["data"]
    csv_path = src["large"]
    url      = src.get("url")
    enc      = src.get("encoding", "utf-8")

    # ── 1. Fast path: parquet cache already exists ──────────────
    if os.path.exists(parquet):
        size_mb = os.path.getsize(parquet) / 1_048_576
        print(f"    [cache hit]  {parquet}  ({size_mb:.1f} MB)")
        return pd.read_parquet(parquet)

    # ── 2. Raw CSV already in large/ ────────────────────────────
    if os.path.exists(csv_path):
        size_mb = os.path.getsize(csv_path) / 1_048_576
        print(f"    [large/ hit] {csv_path}  ({size_mb:.1f} MB) — converting to parquet...")
        df = pd.read_csv(csv_path, encoding=enc, low_memory=False)
        df.to_parquet(parquet, index=False)
        pq_mb = os.path.getsize(parquet) / 1_048_576
        print(f"    Saved parquet → {parquet}  ({pq_mb:.1f} MB)")
        return df

    # ── 3. Download ──────────────────────────────────────────────
    if url:
        print(f"    [download]   {url[:80]}...")
        resp = requests.get(url, timeout=300)
        resp.raise_for_status()
        raw = resp.content.decode(enc, errors="replace")

        # Save raw CSV to large/
        with open(csv_path, "w", encoding="utf-8") as f:
            f.write(raw)
        csv_mb = os.path.getsize(csv_path) / 1_048_576
        print(f"    Saved CSV     → {csv_path}  ({csv_mb:.1f} MB)")

        # Convert and cache as parquet
        df = pd.read_csv(io.StringIO(raw), low_memory=False)
        df.to_parquet(parquet, index=False)
        pq_mb = os.path.getsize(parquet) / 1_048_576
        print(f"    Saved parquet → {parquet}  ({pq_mb:.1f} MB)")
        return df

    # ── 4. Manual download required ─────────────────────────────
    print(f"\n  *** MANUAL DOWNLOAD REQUIRED for '{key}' ***")
    print(f"  {src.get('manual_instructions', f'Place the CSV at: {csv_path}')}")
    print(f"  Then re-run this script.\n")
    return None


# ==============================================================
# SHARED HELPERS
# ==============================================================

def normalise_urn(df):
    """Ensure URN column exists and is a clean string.
    Accepts 'URN' (case-insensitive) or 'school_urn' (EES data catalogue format).
    """
    for col in df.columns:
        if col.upper() == "URN" or col.lower() == "school_urn":
            df = df.rename(columns={col: "URN"})
            df["URN"] = df["URN"].astype(str).str.strip()
            return df
    raise ValueError(f"No URN column found. Columns: {list(df.columns[:10])}")


def select(df, fields, label=""):
    """Keep only requested fields that exist; warn about any missing."""
    available = [f for f in fields if f in df.columns]
    missing   = [f for f in fields if f not in df.columns]
    if missing:
        print(f"    [{label}] Fields not found (skipped): {missing}")
    return df[available].copy()


def merge_report(base, other, label, check_col):
    """Left-join other onto base on URN, report match rate."""
    if other is None or other.empty:
        print(f"  {label}: skipped (not loaded)")
        return base
    overlap = [c for c in other.columns if c in base.columns and c != "URN"]
    if overlap:
        other = other.drop(columns=overlap)
    base["URN"]  = base["URN"].astype(str).str.strip().str.split(".").str[0]
    other["URN"] = other["URN"].astype(str).str.strip().str.split(".").str[0]
    base = base.merge(other, on="URN", how="left")
    if check_col in base.columns:
        n = base[check_col].notna().sum()
        print(f"  {label}: matched {n:,} / {len(base):,} schools")
    return base


# ==============================================================
# STEP 1: GIAS
# ==============================================================

def load_gias():
    """
    GIAS — Get Information About Schools
    Filtered to: open, secondary phase (Secondary / All-through / Middle deemed secondary)

    Field selection agreed in project review (March 2026):

    Identity:
      URN, la_code, la_name, establishment_number, establishment_name

    Type & structure:
      type_of_establishment, establishment_type_group, establishment_status,
      phase_of_education, statutory_low_age, statutory_high_age,
      official_sixth_form, gender, admissions_policy, religious_character

    Capacity (GIAS snapshot — SCAP figures preferred for analysis):
      school_capacity, number_of_pupils, percentage_fsm

    Location:
      postcode, easting, northing, lsoa_code, msoa_code, gor_name, urban_rural
      Note: GIAS carries 2021 LSOA codes — matches IMD 2025 directly, no lookup needed.

    Derived:
      london_sub_region  (Inner London / Outer London / Not London)
    """
    print("\n[1/5] Loading GIAS...")
    df = ensure_parquet("gias")
    if df is None:
        return pd.DataFrame(columns=["URN"])

    print(f"  Total establishments: {len(df):,}")

    # Filter to open secondary schools
    df = df[
        (df["EstablishmentStatus (name)"].str.lower() == "open") &
        (df["PhaseOfEducation (name)"].isin([
            "Secondary", "All-through", "Middle deemed secondary"
        ]))
    ].copy()
    print(f"  After filter (open secondary): {len(df):,}")

    df = normalise_urn(df)

    rename = {
        "LA (code)":                    "la_code",
        "LA (name)":                    "la_name",
        "EstablishmentNumber":          "establishment_number",
        "EstablishmentName":            "establishment_name",
        "TypeOfEstablishment (name)":   "type_of_establishment",
        "EstablishmentTypeGroup (name)":"establishment_type_group",
        "EstablishmentStatus (name)":   "establishment_status",
        "PhaseOfEducation (name)":      "phase_of_education",
        "StatutoryLowAge":              "statutory_low_age",
        "StatutoryHighAge":             "statutory_high_age",
        "OfficialSixthForm (name)":     "official_sixth_form",
        "Gender (name)":                "gender",
        "AdmissionsPolicy (name)":      "admissions_policy",
        "ReligiousCharacter (name)":    "religious_character",
        "SchoolCapacity":               "school_capacity",
        "NumberOfPupils":               "number_of_pupils",
        "PercentageFSM":                "percentage_fsm",
        "Postcode":                     "postcode",
        "Easting":                      "easting",
        "Northing":                     "northing",
        "LSOA (code)":                  "lsoa_code",
        "MSOA (code)":                  "msoa_code",
        "GOR (name)":                   "gor_name",
        "UrbanRural (name)":            "urban_rural",
    }
    df = df.rename(columns={k: v for k, v in rename.items() if k in df.columns})

    # Derived: London sub-region
    LONDON_LA_NAMES = {
        "City of London", "Barking and Dagenham", "Barnet", "Bexley", "Brent",
        "Bromley", "Camden", "Croydon", "Ealing", "Enfield", "Greenwich",
        "Hackney", "Hammersmith and Fulham", "Haringey", "Harrow", "Havering",
        "Hillingdon", "Hounslow", "Islington", "Kensington and Chelsea",
        "Kingston upon Thames", "Lambeth", "Lewisham", "Merton", "Newham",
        "Redbridge", "Richmond upon Thames", "Southwark", "Sutton",
        "Tower Hamlets", "Waltham Forest", "Wandsworth", "Westminster"
    }
    INNER_LONDON = {
        "City of London", "Camden", "Greenwich", "Hackney", "Hammersmith and Fulham",
        "Islington", "Kensington and Chelsea", "Lambeth", "Lewisham", "Newham",
        "Southwark", "Tower Hamlets", "Wandsworth", "Westminster"
    }

    def london_sub_region(la):
        if la in INNER_LONDON:
            return "Inner London"
        elif la in LONDON_LA_NAMES:
            return "Outer London"
        return "Not London"

    if "la_name" in df.columns:
        df["london_sub_region"] = df["la_name"].apply(london_sub_region)

    keep = [
        "URN", "la_code", "la_name", "establishment_number", "establishment_name",
        "type_of_establishment", "establishment_type_group", "establishment_status",
        "phase_of_education", "statutory_low_age", "statutory_high_age",
        "official_sixth_form", "gender", "admissions_policy", "religious_character",
        "school_capacity", "number_of_pupils", "percentage_fsm",
        "postcode", "easting", "northing", "lsoa_code", "msoa_code",
        "gor_name", "urban_rural", "london_sub_region",
    ]
    df = select(df, keep, "GIAS")
    print(f"  Retained {len(df):,} schools, {len(df.columns)} fields")
    return df


# ==============================================================
# STEP 2: Ofsted
# ==============================================================

def load_ofsted():
    """
    Ofsted state-funded school inspections as at 31 August 2025.
    File: State-funded_schools_inspections_and_outcomes_as_at_31_August_2025.csv

    Note: From September 2024 Ofsted no longer issues a single overall grade
    for NEW inspections. This file contains graded inspections carried forward
    from the previous framework; overall_effectiveness will be null for schools
    inspected only under the new report card framework.

    Field selection agreed in project review (March 2026):

    Identity:       URN, laestab
    Context:        idaci_quintile  (Ofsted's own IDACI assignment — useful cross-check)

    Latest graded inspection:
      inspection_number, inspection_type, inspection_start_date, publication_date,
      overall_effectiveness, category_of_concern,
      quality_of_education, behaviour_and_attitudes, personal_development,
      effectiveness_of_leadership_and_management, safeguarding_is_effective,
      sixth_form_provision

    Previous graded inspection (trajectory):
      previous_inspection_start_date, previous_overall_effectiveness,
      previous_category_of_concern, previous_quality_of_education

    Derived:
      ofsted_overall_label  (numeric 1–4 → Outstanding / Good / Requires improvement / Inadequate)
    """
    print("\n[2/5] Loading Ofsted inspection outcomes...")
    df = ensure_parquet("ofsted")
    if df is None or df.empty:
        return pd.DataFrame(columns=["URN"])

    print(f"  Loaded {len(df):,} records")
    df = normalise_urn(df)

    rename = {
        "LAESTAB":                                          "laestab",
        "The income deprivation affecting children index (IDACI) quintile":
                                                            "idaci_quintile",
        "Inspection number of latest graded inspection":   "inspection_number",
        "Inspection type":                                  "inspection_type",
        "Inspection start date":                            "inspection_start_date",
        "Publication date":                                 "publication_date",
        "Overall effectiveness":                            "overall_effectiveness",
        "Category of concern":                              "category_of_concern",
        "Quality of education":                             "quality_of_education",
        "Behaviour and attitudes":                          "behaviour_and_attitudes",
        "Personal development":                             "personal_development",
        "Effectiveness of leadership and management":       "effectiveness_of_leadership_and_management",
        "Safeguarding is effective?":                       "safeguarding_is_effective",
        "Sixth form provision (where applicable)":          "sixth_form_provision",
        "Previous inspection start date":                   "previous_inspection_start_date",
        "Previous graded inspection overall effectiveness": "previous_overall_effectiveness",
        "Previous category of concern":                     "previous_category_of_concern",
        "Previous quality of education":                    "previous_quality_of_education",
    }
    df = df.rename(columns={k: v for k, v in rename.items() if k in df.columns})

    # Keep most recent graded inspection per URN
    if "inspection_start_date" in df.columns:
        df["inspection_start_date"] = pd.to_datetime(
            df["inspection_start_date"], dayfirst=True, errors="coerce"
        )
        df = (df.sort_values("inspection_start_date", ascending=False)
                .drop_duplicates(subset="URN", keep="first"))

    # Numeric → label for overall effectiveness
    rating_map = {1: "Outstanding", 2: "Good", 3: "Requires improvement", 4: "Inadequate"}
    if "overall_effectiveness" in df.columns:
        df["overall_effectiveness"] = pd.to_numeric(
            df["overall_effectiveness"], errors="coerce"
        )
        df["ofsted_overall_label"] = df["overall_effectiveness"].map(rating_map)

    keep = [
        "URN", "laestab", "idaci_quintile",
        "inspection_number", "inspection_type",
        "inspection_start_date", "publication_date",
        "overall_effectiveness", "ofsted_overall_label", "category_of_concern",
        "quality_of_education", "behaviour_and_attitudes", "personal_development",
        "effectiveness_of_leadership_and_management", "safeguarding_is_effective",
        "sixth_form_provision",
        "previous_inspection_start_date", "previous_overall_effectiveness",
        "previous_category_of_concern", "previous_quality_of_education",
    ]
    df = select(df, keep, "Ofsted")
    print(f"  Retained {len(df):,} unique schools, {len(df.columns)} fields")
    return df


# ==============================================================
# STEP 3: KS4 Performance
# ==============================================================

def load_ks4():
    """
    KS4 school-level performance — 2023/24 final data.
    File: 202324_performance_tables_schools_final.csv

    This is the LAST year with Progress 8 available.
    Progress 8 is NOT published for 2024/25 or 2025/26 (COVID KS2 gap).

    The file contains multiple rows per school by pupil characteristic
    (breakdown field). We keep ALL-PUPILS rows only.

    Field selection agreed in project review (March 2026):

    Core attainment:
      ks4_attainment8             avg_att8
      ks4_pct_grade4_eng_maths    pt_l2basics_94
      ks4_pct_grade5_eng_maths    pt_l2basics_95

    Progress 8:
      ks4_progress8               avg_p8score
      ks4_p8_lower_ci             p8score_ci_low
      ks4_p8_upper_ci             p8score_ci_upp
      ks4_pct_in_p8_calc          pt_inp8calc   (data quality flag)

    EBacc:
      ks4_pct_entering_ebacc      pt_ebacc_e_ptq_ee
      ks4_pct_achieving_ebacc     pt_ebacc_94
      ks4_ebacc_aps               avg_ebaccaps

    Pupils:
      ks4_total_pupils            t_pupils
    """
    print("\n[3/5] Loading KS4 performance (2023/24)...")
    df = ensure_parquet("ks4")
    if df is None or df.empty:
        return pd.DataFrame(columns=["URN"])

    print(f"  Loaded {len(df):,} rows (all breakdowns)")
    df = normalise_urn(df)

    # Keep all-pupils rows only
    if "breakdown" in df.columns:
        all_pupils_vals = [
            v for v in df["breakdown"].dropna().unique()
            if str(v).lower() in ("all", "all pupils", "total")
        ]
        if all_pupils_vals:
            df = df[df["breakdown"].isin(all_pupils_vals)].copy()
            print(f"  After filtering to all-pupils rows: {len(df):,}")
        else:
            most_common = df["breakdown"].value_counts().idxmax()
            df = df[df["breakdown"] == most_common].copy()
            print(f"  Breakdown values found: {list(df['breakdown'].unique()[:5])}")
            print(f"  Filtered to most common breakdown '{most_common}': {len(df):,} rows")

    rename = {
        "avg_att8":          "ks4_attainment8",
        "pt_l2basics_94":    "ks4_pct_grade4_eng_maths",
        "pt_l2basics_95":    "ks4_pct_grade5_eng_maths",
        "avg_p8score":       "ks4_progress8",
        "p8score_ci_low":    "ks4_p8_lower_ci",
        "p8score_ci_upp":    "ks4_p8_upper_ci",
        "pt_inp8calc":       "ks4_pct_in_p8_calc",
        "pt_ebacc_e_ptq_ee": "ks4_pct_entering_ebacc",
        "pt_ebacc_94":       "ks4_pct_achieving_ebacc",
        "avg_ebaccaps":      "ks4_ebacc_aps",
        "t_pupils":          "ks4_total_pupils",
    }
    df = df.rename(columns={k: v for k, v in rename.items() if k in df.columns})

    keep = [
        "URN",
        "ks4_attainment8", "ks4_pct_grade4_eng_maths", "ks4_pct_grade5_eng_maths",
        "ks4_progress8", "ks4_p8_lower_ci", "ks4_p8_upper_ci", "ks4_pct_in_p8_calc",
        "ks4_pct_entering_ebacc", "ks4_pct_achieving_ebacc", "ks4_ebacc_aps",
        "ks4_total_pupils",
    ]
    df = select(df, keep, "KS4")

    for col in df.columns:
        if col != "URN":
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.drop_duplicates("URN")
    print(f"  Retained {len(df):,} schools, {len(df.columns)} fields")
    return df


# ==============================================================
# STEP 4: SCAP — Capacity & Utilisation
# ==============================================================

def load_scap():
    """
    SCAP 2023/24 — School Capacity Survey (as at 1 May 2024).

    Two files:
      Mainstream: capacity_school_200910-202324.csv
      Sixth form: sixth-form-capacity_school_201718-202324.csv

    Both span multiple years — filtered to 2023/24 rows only.

    Mainstream fields:
      scap_school_places            school_places
      scap_pupils_on_roll           pupils_on_roll
      scap_unfilled_places          unfilled_places
      scap_pct_unfilled             percent_unfilled_places
      scap_pupils_over_capacity     pupils_over_capacity
      scap_pct_over_capacity        percent_pupils_over_capacity

    Derived:
      scap_utilisation_pct  = pupils_on_roll / school_places × 100

    Sixth form fields (total_pupils_on_roll and total_school_places dropped — duplicates):
      sf_capacity           reported_sixth_form_places
      sf_pupils_on_roll     sixth_form_pupils_on_roll

    Derived:
      sf_utilisation_pct  = sf_pupils_on_roll / sf_capacity × 100
    """
    print("\n[4/5] Loading SCAP capacity data (2023/24)...")

    # --- Mainstream ---
    df_main = ensure_parquet("scap_mainstream")
    if df_main is None or df_main.empty:
        return pd.DataFrame(columns=["URN"])

    print(f"  Mainstream: {len(df_main):,} rows (all years, all phases)")
    df_main = normalise_urn(df_main)

    year_col = next((c for c in df_main.columns
                     if "time_period" in c.lower() or "year" in c.lower()), None)
    if year_col:
        df_main = df_main[
            df_main[year_col].astype(str).str.contains("2023", na=False)
        ].copy()
        print(f"  After 2023/24 filter: {len(df_main):,} rows")

    rename_main = {
        "school_places":               "scap_school_places",
        "pupils_on_roll":              "scap_pupils_on_roll",
        "unfilled_places":             "scap_unfilled_places",
        "percent_unfilled_places":     "scap_pct_unfilled",
        "pupils_over_capacity":        "scap_pupils_over_capacity",
        "percent_pupils_over_capacity":"scap_pct_over_capacity",
    }
    df_main = df_main.rename(
        columns={k: v for k, v in rename_main.items() if k in df_main.columns}
    )

    for col in ["scap_school_places", "scap_pupils_on_roll"]:
        if col in df_main.columns:
            df_main[col] = pd.to_numeric(df_main[col], errors="coerce")

    if "scap_school_places" in df_main.columns and "scap_pupils_on_roll" in df_main.columns:
        df_main["scap_utilisation_pct"] = (
            df_main["scap_pupils_on_roll"] / df_main["scap_school_places"] * 100
        ).round(1)

    keep_main = [
        "URN", "scap_school_places", "scap_pupils_on_roll",
        "scap_unfilled_places", "scap_pct_unfilled",
        "scap_pupils_over_capacity", "scap_pct_over_capacity",
        "scap_utilisation_pct",
    ]
    df_main = select(df_main, keep_main, "SCAP mainstream")
    df_main = df_main.drop_duplicates("URN")

    # --- Sixth form ---
    df_sf = ensure_parquet("scap_sixthform")
    if df_sf is not None and not df_sf.empty:
        print(f"  Sixth form: {len(df_sf):,} rows (all years)")
        df_sf = normalise_urn(df_sf)

        year_col_sf = next((c for c in df_sf.columns
                            if "time_period" in c.lower() or "year" in c.lower()), None)
        if year_col_sf:
            df_sf = df_sf[
                df_sf[year_col_sf].astype(str).str.contains("2023", na=False)
            ].copy()
            print(f"  Sixth form after 2023/24 filter: {len(df_sf):,} rows")

        rename_sf = {
            "reported_sixth_form_places": "sf_capacity",
            "sixth_form_pupils_on_roll":  "sf_pupils_on_roll",
        }
        df_sf = df_sf.rename(
            columns={k: v for k, v in rename_sf.items() if k in df_sf.columns}
        )

        for col in ["sf_capacity", "sf_pupils_on_roll"]:
            if col in df_sf.columns:
                df_sf[col] = pd.to_numeric(df_sf[col], errors="coerce")

        if "sf_capacity" in df_sf.columns and "sf_pupils_on_roll" in df_sf.columns:
            df_sf["sf_utilisation_pct"] = (
                df_sf["sf_pupils_on_roll"] / df_sf["sf_capacity"] * 100
            ).round(1)

        keep_sf = ["URN", "sf_capacity", "sf_pupils_on_roll", "sf_utilisation_pct"]
        df_sf = select(df_sf, keep_sf, "SCAP sixth form")
        df_sf = df_sf.drop_duplicates("URN")
        df_main["URN"] = df_main["URN"].astype(str).str.strip().str.split(".").str[0]
        df_sf["URN"]   = df_sf["URN"].astype(str).str.strip().str.split(".").str[0]
        df_main = df_main.merge(df_sf, on="URN", how="left")
        
        n_sf = df_main["sf_capacity"].notna().sum() if "sf_capacity" in df_main.columns else 0
        print(f"  Sixth form matched: {n_sf:,} schools with sixth form capacity data")
    else:
        print("  Sixth form: skipped (not loaded)")

    print(f"  SCAP final: {len(df_main):,} schools, {len(df_main.columns)} fields")
    return df_main


# ==============================================================
# STEP 5: IMD 2025
# ==============================================================

def load_imd():
    """
    Index of Multiple Deprivation 2025 — File 7
    File: File_7_IoD2025_All_Ranks_Scores_Deciles_Population_Denominators.csv
    Published October 2025. Uses 2021 LSOA boundaries — matches GIAS directly.

    Joined to schools on lsoa_code (already in GIAS — no postcode lookup needed).

    Fields retained:
      lsoa_code             Join key (2021 LSOA)
      imd_score             Raw score (higher = more deprived)
      imd_rank              Rank 1 (most deprived) to 33,755
      imd_decile            1 (most deprived 10%) to 10 (least deprived 10%)
      idaci_score           Income Deprivation Affecting Children Index score
      idaci_decile          IDACI decile — preferred measure for school research
      imd_education_score   Education, Skills & Training domain score
      imd_education_decile  Education domain decile

    Decile convention: 1 = most deprived, 10 = least deprived (national)
    """
    print("\n[5/5] Loading IMD 2025...")
    df = ensure_parquet("imd")
    if df is None or df.empty:
        return None

    print(f"  Loaded {len(df):,} LSOA records")

    # File 7 has verbose column headers — detect by substring match
    rename = {}
    for col in df.columns:
        cl = col.lower()
        if "lsoa code" in cl:
            rename[col] = "lsoa_code"
        elif "index of multiple deprivation (imd) score" in cl:
            rename[col] = "imd_score"
        elif "index of multiple deprivation (imd) rank" in cl:
            rename[col] = "imd_rank"
        elif "index of multiple deprivation (imd) decile" in cl:
            rename[col] = "imd_decile"
        elif "income deprivation affecting children index (idaci) score" in cl:
            rename[col] = "idaci_score"
        elif "income deprivation affecting children index (idaci) decile" in cl:
            rename[col] = "idaci_decile"
        elif "education, skills and training score" in cl:
            rename[col] = "imd_education_score"
        elif "education, skills and training decile" in cl:
            rename[col] = "imd_education_decile"

    df = df.rename(columns=rename)

    keep = [
        "lsoa_code", "imd_score", "imd_rank", "imd_decile",
        "idaci_score", "idaci_decile",
        "imd_education_score", "imd_education_decile",
    ]
    df = select(df, keep, "IMD")

    if "lsoa_code" not in df.columns:
        print("  WARNING: lsoa_code not found — check File 7 column headers")
        print(f"  Available columns: {list(df.columns[:10])}")
        return None

    df = df.drop_duplicates("lsoa_code")
    print(f"  IMD 2025: {len(df):,} LSOAs, {len(df.columns)} fields")
    return df


# ==============================================================
# STEP 6: School Workforce — Pupil:Teacher Ratios
# ==============================================================

def load_workforce():
    """
    School Workforce in England — Pupil to teacher ratios, school level.
    Reporting Year 2024 (2024/25 academic year).
    Published June 2025.

    Source: DfE School Workforce in England
    https://explore-education-statistics.service.gov.uk/data-catalogue/data-set/12849f4a-1427-4dc2-9f7a-e069087824aa

    The file covers multiple years (2010/11–2024/25) and all school types.
    Filtered to: time_period == 202425 (most recent year only).

    Fields retained:
      wf_pupils_fte                       Total full-time equivalent pupils
      wf_qualified_teachers_fte           Full-time equivalent qualified teachers
      wf_teachers_fte                     Full-time equivalent all teachers (qualified + unqualified)
      wf_pupil_to_qual_unqual_teacher_ratio  Pupils per teacher (qualified and unqualified combined)
    """
    print("\n[6/6] Loading School Workforce — pupil:teacher ratios (2024/25)...")
    df = ensure_parquet("workforce")
    if df is None or df.empty:
        return pd.DataFrame(columns=["URN"])

    print(f"  Loaded {len(df):,} rows (all years, all school types)")
    df = normalise_urn(df)

    # Filter to most recent year only
    year_col = next((c for c in df.columns
                     if "time_period" in c.lower()), None)
    if year_col:
        df = df[df[year_col].astype(str).str.contains("202425", na=False)].copy()
        print(f"  After 2024/25 filter: {len(df):,} rows")

    rename = {
        "pupils_fte":                          "wf_pupils_fte",
        "qualified_teachers_fte":              "wf_qualified_teachers_fte",
        "teachers_fte":                        "wf_teachers_fte",
        "pupil_to_qual_unqual_teacher_ratio":  "wf_pupil_to_qual_unqual_teacher_ratio",
    }
    df = df.rename(columns={k: v for k, v in rename.items() if k in df.columns})

    keep = [
        "URN",
        "wf_pupils_fte",
        "wf_qualified_teachers_fte",
        "wf_teachers_fte",
        "wf_pupil_to_qual_unqual_teacher_ratio",
    ]
    df = select(df, keep, "Workforce")

    for col in df.columns:
        if col != "URN":
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.drop_duplicates("URN")
    print(f"  Retained {len(df):,} schools, {len(df.columns)} fields")
    return df


# ==============================================================
# STEP 7: GLA Boundary (spatial layer)
# ==============================================================

def load_gla_boundary():
    """
    GLA boundary — Greater London Authority boundary shapefile.
    Downloaded as a zip containing a shapefile.

    Used to derive top_att8_quartile_GLA and top_p8_quartile_GLA:
      Schools within GLA boundary + 5 km buffer, with non-null Att8/P8,
      flagged 1 if in the top 25 %, else 0.

    Returns:
      gpd.GeoDataFrame with the GLA boundary in British National Grid (EPSG:27700),
      or None if unavailable.
    """
    print("\n[7/7] Loading GLA boundary...")
    src      = SOURCES["gla_boundary"]
    parquet  = src["data"]
    zip_path = src["large"]
    url      = src.get("url")

    # ── 1. Fast path: parquet cache already exists ────────────
    if os.path.exists(parquet):
        size_mb = os.path.getsize(parquet) / 1_048_576
        print(f"    [cache hit]  {parquet}  ({size_mb:.1f} MB)")
        gdf = gpd.read_parquet(parquet)
        return gdf

    # ── 2. Zip already in large/ ──────────────────────────────
    if not os.path.exists(zip_path):
        if url:
            print(f"    [download]   {url[:80]}...")
            resp = requests.get(url, timeout=300)
            resp.raise_for_status()
            with open(zip_path, "wb") as f:
                f.write(resp.content)
            zip_mb = os.path.getsize(zip_path) / 1_048_576
            print(f"    Saved zip     → {zip_path}  ({zip_mb:.1f} MB)")
        else:
            print(f"\n  *** MANUAL DOWNLOAD REQUIRED for 'gla_boundary' ***")
            print(f"  Place the zip at: {zip_path}")
            return None

    # ── 3. Read shapefile from zip ────────────────────────────
    # Extract to a temp folder so pyogrio gets an absolute path on all platforms
    print(f"    [large/ hit] {zip_path} — extracting and reading shapefile...")
    extract_dir = os.path.join(DIR_LARGE, "gla_extracted")
    abs_zip = os.path.abspath(zip_path)
    with zipfile.ZipFile(abs_zip, "r") as zf:
        zf.extractall(extract_dir)

    # Find the .shp file inside the extracted folder
    shp_files = [
        os.path.join(root, f)
        for root, _, files in os.walk(extract_dir)
        for f in files if f.lower().endswith(".shp")
    ]
    if not shp_files:
        print("  ERROR: no .shp file found inside gla.zip")
        return None
    shp_path = shp_files[0]
    print(f"    Reading: {shp_path}")
    gdf = gpd.read_file(shp_path)

    # Ensure British National Grid
    if gdf.crs is None:
        gdf = gdf.set_crs(epsg=27700)
    elif gdf.crs.to_epsg() != 27700:
        gdf = gdf.to_crs(epsg=27700)

    gdf.to_parquet(parquet, index=False)
    pq_mb = os.path.getsize(parquet) / 1_048_576
    print(f"    Saved parquet → {parquet}  ({pq_mb:.1f} MB)")
    return gdf


# ==============================================================
# DERIVED FIELDS
# ==============================================================

def add_derived_fields(db, gla_boundary=None):
    """
    Compute analytical derived fields after all joins are complete.

    1. ks4_attainment8_england_quartile  — Attainment 8 quartile within England
    2. ks4_p8_category                  — Progress 8 banded category
    3. intake_neighbourhood_gap         — School FSM% vs neighbourhood IDACI
                                          (Sub-RQ 2 key variable; uses GIAS percentage_fsm
                                           as fallback — full version pending pupil
                                           characteristics data source)
    4. top_att8_quartile_GLA            — 1 if school is in top 25 % of Att8 scores
                                          among schools within GLA boundary + 5 km buffer
                                          (excluding nulls), else 0
    5. top_p8_quartile_GLA             — 1 if school is in top 25 % of Progress 8 scores
                                          among schools within GLA boundary + 5 km buffer
                                          (excluding nulls), else 0
    """
    print("\n[Derived fields]")

    # Attainment 8 quartile (England-wide)
    if "ks4_attainment8" in db.columns:
        db["ks4_attainment8_england_quartile"] = pd.qcut(
            db["ks4_attainment8"],
            q=4,
            labels=["Q1 (lowest)", "Q2", "Q3", "Q4 (highest)"],
            duplicates="drop"
        )
        print("  ks4_attainment8_england_quartile: computed")

    # Progress 8 banded category
    if "ks4_progress8" in db.columns:
        db["ks4_p8_category"] = pd.cut(
            db["ks4_progress8"],
            bins=[-float("inf"), -0.5, -0.1, 0.1, 0.5, float("inf")],
            labels=[
                "Well below avg (<-0.5)",
                "Below avg (-0.5 to -0.1)",
                "Average (-0.1 to +0.1)",
                "Above avg (+0.1 to +0.5)",
                "Well above avg (>+0.5)"
            ]
        )
        print("  ks4_p8_category: computed")

    # Intake–neighbourhood gap
    if "idaci_decile" in db.columns:
        # Convert IDACI decile to approximate 0–100 deprivation scale
        # Decile 1 = most deprived ≈ 90–100%, decile 10 = least deprived ≈ 0–10%
        db["idaci_pct_approx"] = (
            (11 - pd.to_numeric(db["idaci_decile"], errors="coerce")) * 10
        )
        fsm_col = next(
            (c for c in ["pct_fsm_ever6yrs", "percentage_fsm"] if c in db.columns),
            None
        )
        if fsm_col:
            db["intake_neighbourhood_gap"] = (
                pd.to_numeric(db[fsm_col], errors="coerce") - db["idaci_pct_approx"]
            )
            print(f"  intake_neighbourhood_gap: computed (using {fsm_col})")

    # ── GLA top-quartile flags ────────────────────────────────
    # Schools within GLA boundary + 5 km buffer, non-null scores,
    # top 25 % flagged as 1, rest as 0.
    db["top_att8_quartile_GLA"] = 0
    db["top_p8_quartile_GLA"]   = 0

    if gla_boundary is not None and "easting" in db.columns and "northing" in db.columns:
        print("\n  [GLA top-quartile derivation]")

        # Build school points (BNG) — only rows with valid coordinates
        east = pd.to_numeric(db["easting"], errors="coerce")
        north = pd.to_numeric(db["northing"], errors="coerce")
        has_coords = east.notna() & north.notna()

        if has_coords.any():
            geom = [Point(e, n) for e, n in zip(east[has_coords], north[has_coords])]
            schools_gdf = gpd.GeoDataFrame(
                db.loc[has_coords].copy(),
                geometry=geom,
                crs="EPSG:27700",
            )

            # Dissolve GLA boundary to single polygon and buffer by 5 km
            gla_dissolved = gla_boundary.dissolve().to_crs(epsg=27700)
            gla_buffered  = gla_dissolved.buffer(5000).union_all()   # 5 km buffer
            print(f"    GLA boundary buffered by 5 km")

            # Spatial filter: schools within the buffered boundary
            in_gla = schools_gdf.within(gla_buffered)
            gla_schools = schools_gdf.loc[in_gla]
            print(f"    Schools within GLA + 5 km buffer: {len(gla_schools):,}")

            # --- Top Att8 quartile ---
            if "ks4_attainment8" in gla_schools.columns:
                att8_valid = gla_schools["ks4_attainment8"].dropna()
                if len(att8_valid) >= 4:
                    threshold_att8 = att8_valid.quantile(0.75)
                    top_att8_idx = gla_schools.loc[
                        gla_schools["ks4_attainment8"].notna()
                        & (gla_schools["ks4_attainment8"] >= threshold_att8)
                    ].index
                    db.loc[top_att8_idx, "top_att8_quartile_GLA"] = 1
                    print(f"    top_att8_quartile_GLA: {len(top_att8_idx):,} schools flagged "
                          f"(threshold >= {threshold_att8:.1f})")
                else:
                    print("    top_att8_quartile_GLA: too few valid scores to compute quartile")

            # --- Top P8 quartile ---
            if "ks4_progress8" in gla_schools.columns:
                p8_valid = gla_schools["ks4_progress8"].dropna()
                if len(p8_valid) >= 4:
                    threshold_p8 = p8_valid.quantile(0.75)
                    top_p8_idx = gla_schools.loc[
                        gla_schools["ks4_progress8"].notna()
                        & (gla_schools["ks4_progress8"] >= threshold_p8)
                    ].index
                    db.loc[top_p8_idx, "top_p8_quartile_GLA"] = 1
                    print(f"    top_p8_quartile_GLA: {len(top_p8_idx):,} schools flagged "
                          f"(threshold >= {threshold_p8:.2f})")
                else:
                    print("    top_p8_quartile_GLA: too few valid scores to compute quartile")
    else:
        if gla_boundary is None:
            print("  GLA top-quartile: skipped (GLA boundary not loaded)")
        else:
            print("  GLA top-quartile: skipped (easting/northing missing)")

    return db


# ==============================================================
# MAIN
# ==============================================================

def build_database():
    print("=" * 65)
    print("  England Secondary Schools Database Builder")
    print("=" * 65)
    print(f"\n  Cache layout:")
    print(f"    large/  — raw CSVs (downloaded once, kept for reference)")
    print(f"    data/   — parquet cache (fast reads on subsequent runs)")

    # Load all sources — each resolves via ensure_parquet()
    gias         = load_gias()
    ofsted       = load_ofsted()
    ks4          = load_ks4()
    scap         = load_scap()
    imd          = load_imd()
    workforce    = load_workforce()
    gla_boundary = load_gla_boundary()

    if gias is None or gias.empty or len(gias) <= 1:
        print(
            "\nERROR: GIAS is the base table and could not be loaded.\n"
            "       Place the GIAS CSV at large/gias_establishments.csv and re-run."
        )
        return None

    # Merge on URN
    print("\n[Merging on URN...]")
    db = gias.copy()
    db = merge_report(db, ofsted,    "Ofsted",    "overall_effectiveness")
    db = merge_report(db, ks4,       "KS4",       "ks4_attainment8")
    db = merge_report(db, scap,      "SCAP",      "scap_utilisation_pct")
    db = merge_report(db, workforce, "Workforce", "wf_pupil_to_qual_unqual_teacher_ratio")

    # Join IMD 2025 on lsoa_code (2021 boundaries — matches GIAS directly, no lookup needed)
    if imd is not None and "lsoa_code" in db.columns:
        print("\n[Joining IMD 2025 on lsoa_code...]")
        db = db.merge(imd, on="lsoa_code", how="left")
        n = db["imd_decile"].notna().sum() if "imd_decile" in db.columns else 0
        print(f"  IMD matched: {n:,} / {len(db):,} schools")
    else:
        print("\n  IMD join skipped (not loaded or lsoa_code missing from GIAS)")

    # Derived fields
    db = add_derived_fields(db, gla_boundary=gla_boundary)

    # Save outputs
    print(f"\n[Saving outputs...]")
    db.to_parquet(OUTPUT_PARQUET, index=False)
    db.to_csv(OUTPUT_CSV, index=False)
    out_mb = os.path.getsize(OUTPUT_PARQUET) / 1_048_576
    print(f"  Parquet: {OUTPUT_PARQUET}  ({out_mb:.1f} MB)")
    print(f"  CSV:     {OUTPUT_CSV}")
    print(f"  Schools: {len(db):,}  |  Columns: {len(db.columns)}")

    # Summary
    print("\n[Summary]")
    if "gor_name" in db.columns:
        print(f"\n  Schools by region:\n{db['gor_name'].value_counts().to_string()}")
    if "london_sub_region" in db.columns:
        print(f"\n  London sub-region:\n{db['london_sub_region'].value_counts().to_string()}")
    if "type_of_establishment" in db.columns:
        print(f"\n  School types:\n{db['type_of_establishment'].value_counts().head(10).to_string()}")
    if "ofsted_overall_label" in db.columns:
        print(f"\n  Ofsted ratings:\n{db['ofsted_overall_label'].value_counts().to_string()}")
    if "ks4_attainment8" in db.columns:
        att8 = db["ks4_attainment8"].dropna()
        print(f"\n  Attainment 8: n={len(att8):,}  mean={att8.mean():.1f}  "
              f"median={att8.median():.1f}  min={att8.min():.1f}  max={att8.max():.1f}")
    if "ks4_progress8" in db.columns:
        p8 = db["ks4_progress8"].dropna()
        print(f"  Progress 8:   n={len(p8):,}  mean={p8.mean():.2f}  "
              f"median={p8.median():.2f}  min={p8.min():.2f}  max={p8.max():.2f}")

    print("\n[London filter example]")
    print("  london = df[df['london_sub_region'].isin(['Inner London', 'Outer London'])]")

    return db


if __name__ == "__main__":
    db = build_database()