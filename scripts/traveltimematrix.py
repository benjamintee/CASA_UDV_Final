"""
r5py travel time matrix — London secondary schools accessibility
================================================================
Modes:       TRANSIT+WALK, WALK only, CYCLE only, CAR only (free-flow)
Origins:     4,994 London LSOA population weighted centroids
Destinations: All secondary schools within 5km buffer of GLA boundary
Departure:   Tuesday 17 March 2026, 08:30, 60-min window, median
Targets:
  (1) Nearest any school
  (2) Nearest Outstanding school
  (3) Nearest top-25% Attainment 8 school
  (4) Nearest top-25% Progress 8 school
 
Note: car times are free-flow (OSM speed limits, no congestion).
AM peak actual times likely 1.3-1.5x longer. Use with caveat.
"""

import r5py
import geopandas as gpd
import pandas as pd
import numpy as np
from datetime import datetime
from tqdm import tqdm
import os

os.makedirs("data", exist_ok=True)
os.makedirs("outputs/figures", exist_ok=True)

# ── 0. Paths ─────────────────────────────────────────────────────────────────

OSM_PATH = "large/greater-london-latest.osm.pbf"
GTFS_PATHS = [
    "large/itm_london_gtfs.zip",
    "large/national_rail_fixed_gtfs.zip",
]
LSOA_PATH = "data/london_lsoa_pwc.gpkg"
SCHOOLS_PATH = "england_secondary_schools.parquet"
GLA_PATH = "data/London_GLA_Boundary.shp"
OUTPUT_PATH = "data/lsoa_travel_times.parquet"

DEPARTURE = datetime(2026, 3, 17, 8, 30)
MAX_MINS = 90
PERCENTILE = 50
BUFFER_M = 5_000   # 5km buffer around GLA for destination schools
BATCH_SIZE = 200     # origins per batch

# ── 1. Load origins ───────────────────────────────────────────────────────────

print("Loading LSOA centroids...")
origins = gpd.read_file(LSOA_PATH)[["LSOA21CD", "geometry"]].copy()
origins = origins.rename(columns={"LSOA21CD": "id"})
origins = origins.to_crs("EPSG:4326")
print(f"  {len(origins):,} origins")

# ── 2. Load destinations ──────────────────────────────────────────────────────

print("Loading GLA boundary and applying buffer...")
gla = gpd.read_file(GLA_PATH).to_crs("EPSG:27700")
gla_buffered = gla.copy()
gla_buffered["geometry"] = gla.buffer(BUFFER_M)
print(f"  Buffer: {BUFFER_M/1000:.0f}km around GLA")

print("Loading schools...")
schools = pd.read_parquet(SCHOOLS_PATH)
schools_gdf = gpd.GeoDataFrame(
    schools,
    geometry=gpd.points_from_xy(schools["easting"], schools["northing"]),
    crs="EPSG:27700"
)

# --- FILTER SCHOOLS IN GREATER LONDON WITH 5KM BUFFER HERE ---
schools_buffered = gpd.clip(schools_gdf, gla_buffered).copy()
print(f"  {len(schools_buffered):,} schools within {BUFFER_M/1000:.0f}km of GLA")

# --- FILTER SCHOOLS IN LONDON HERE ---
london_mask = schools_buffered["london_sub_region"].isin(
    ["Inner London", "Outer London"])
london_schools = schools_buffered[london_mask]
print(f"  of which {len(london_schools):,} are London schools")

# --- FILTER SCHOOLS WITHOUT SELECTIVE ADMISSION HERE ---
excluded_policies = ["Selective", "Not applicable"]
schools_buffered = schools_buffered[~schools_buffered["admissions_policy"].isin(
    excluded_policies)].copy()
print(f"  {len(schools_buffered):,} schools remaining after excluding Selective/NA policies")

# ------------------------------

# ── 3. Build destinations GeoDataFrame ────────────────────────────────────────

destinations = schools_buffered[["URN", "geometry",
                                 "ofsted_overall_label",
                                 "ks4_attainment8",
                                 "ks4_progress8"]].copy()
destinations = destinations.rename(columns={"URN": "urn"})
destinations["id"] = destinations["urn"].astype(str)
destinations = destinations.to_crs("EPSG:4326")

# ── 4. Tag destination tiers ──────────────────────────────────────────────────

att8_threshold = schools_buffered["ks4_attainment8"].quantile(0.75)
p8_threshold = schools_buffered["ks4_progress8"].quantile(0.75)

print(
    f"\nDestination thresholds (all schools within {BUFFER_M/1000:.0f}km of GLA):")
print(f"  Top 25% Attainment 8 cutoff : {att8_threshold:.1f}")
print(f"  Top 25% Progress 8 cutoff   : {p8_threshold:.2f}")
print(
    f"  Outstanding schools (buffer) : {(destinations['ofsted_overall_label'] == 'Outstanding').sum()}")
print(
    f"  Top 25% Att8 (buffer)        : {(destinations['ks4_attainment8'] >= att8_threshold).sum()}")
print(
    f"  Top 25% P8 (buffer)          : {(destinations['ks4_progress8'] >= p8_threshold).sum()}")


school_tags = destinations.set_index("id")[[
    "ofsted_overall_label",
    "ks4_attainment8",
    "ks4_progress8",
]].copy()
school_tags["is_outstanding"] = school_tags["ofsted_overall_label"] == "Outstanding"
school_tags["is_top25_att8"] = school_tags["ks4_attainment8"] >= att8_threshold
school_tags["is_top25_p8"] = school_tags["ks4_progress8"] >= p8_threshold

dest_gdf = destinations[["id", "geometry"]].copy()

# ── 5. Build transport network ────────────────────────────────────────────────

print("\nBuilding transport network (this takes a few minutes)...")
transport_network = r5py.TransportNetwork(OSM_PATH, GTFS_PATHS)
print("✓ Network built")

# ── 6. Compute travel time matrices in batches ────────────────────────────────


def run_matrix_batched(network, origins, destinations, mode_label):
    n = len(origins)
    batches = [origins.iloc[i:i+BATCH_SIZE] for i in range(0, n, BATCH_SIZE)]
    print(f"\nComputing {mode_label} matrix "
          f"({n:,} origins x {len(destinations):,} destinations) "
          f"in {len(batches)} batches of {BATCH_SIZE}...")

    if mode_label == "transit":
        modes = [r5py.TransportMode.TRANSIT, r5py.TransportMode.WALK]
    elif mode_label == "cycle":
        modes = [r5py.TransportMode.BICYCLE]
    elif mode_label == "car":
        modes = [r5py.TransportMode.CAR]
    else:
        modes = [r5py.TransportMode.WALK]

    results = []
    for batch_origins in tqdm(batches, desc=mode_label, unit="batch"):
        computer = r5py.TravelTimeMatrix(
            network,
            origins=batch_origins,
            destinations=destinations,
            departure=DEPARTURE,
            departure_time_window=pd.Timedelta(hours=1),
            max_time=pd.Timedelta(minutes=MAX_MINS),
            transport_modes=modes,
            percentiles=[PERCENTILE],
        )
        results.append(computer[["from_id", "to_id", "travel_time"]])

    full = pd.concat(results, ignore_index=True)
    nan_count = full["travel_time"].isna().sum()
    print(f"  ✓ {len(full):,} pairs | NaN (unreachable): {nan_count:,} "
          f"({nan_count/len(full)*100:.1f}%)")
    return full


transit_tt = run_matrix_batched(
    transport_network, origins, dest_gdf, "transit")
transit_tt.to_parquet("large/raw_transit_tt.parquet", index=False)
print("✓ raw transit saved")

walk_tt = run_matrix_batched(transport_network, origins, dest_gdf, "walk")
walk_tt.to_parquet("large/raw_walk_tt.parquet", index=False)
print("✓ raw walk saved")

cycle_tt = run_matrix_batched(transport_network, origins, dest_gdf, "cycle")
cycle_tt.to_parquet("large/raw_cycle_tt.parquet", index=False)
print("✓ raw cycle saved")

car_tt = run_matrix_batched(transport_network, origins, dest_gdf, "car")
car_tt.to_parquet("large/raw_car_tt.parquet", index=False)
print("✓ raw car saved")

# ── 7. Summarise into per-LSOA metrics ───────────────────────────────────────


def summarise(tt_matrix, school_tags, mode_label):
    df = tt_matrix.rename(columns={
        "from_id":     "lsoa_id",
        "to_id":       "school_id",
        "travel_time": "tt"
    })
    df = df.dropna(subset=["tt"])
    df = df.join(school_tags, on="school_id", how="left")

    records = []
    for lsoa_id, grp in tqdm(
        df.groupby("lsoa_id"),
        desc=f"summarise {mode_label}",
        unit="LSOA"
    ):
        row = {"lsoa_id": lsoa_id}

        row[f"tt_{mode_label}_nearest_any"] = grp["tt"].min()

        sub = grp[grp["is_outstanding"]]
        row[f"tt_{mode_label}_nearest_outstanding"] = (
            sub["tt"].min() if len(sub) else np.nan
        )

        sub = grp[grp["is_top25_att8"]]
        row[f"tt_{mode_label}_nearest_top25_att8"] = (
            sub["tt"].min() if len(sub) else np.nan
        )

        sub = grp[grp["is_top25_p8"]]
        row[f"tt_{mode_label}_nearest_top25_p8"] = (
            sub["tt"].min() if len(sub) else np.nan
        )

        records.append(row)

    result = pd.DataFrame(records)
    print(f"\n{mode_label} summary ({len(result):,} LSOAs):")
    print(result.describe().round(1))
    return result


transit_metrics = summarise(transit_tt, school_tags, "transit")
walk_metrics = summarise(walk_tt,    school_tags, "walk")
cycle_metrics = summarise(cycle_tt,   school_tags, "cycle")
car_metrics = summarise(car_tt,     school_tags, "car")

# ── 8. Merge and save ─────────────────────────────────────────────────────────

lsoa_metrics = (
    transit_metrics
    .merge(walk_metrics,  on="lsoa_id", how="outer")
    .merge(cycle_metrics, on="lsoa_id", how="outer")
    .merge(car_metrics,   on="lsoa_id", how="outer")
)

lsoa_metrics.to_parquet(OUTPUT_PATH, index=False)
print(f"\n✓ Saved to {OUTPUT_PATH}")
print(f"  Shape:   {lsoa_metrics.shape}")
print(f"  Columns: {lsoa_metrics.columns.tolist()}")
