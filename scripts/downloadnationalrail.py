## Script for downloading different GTFS feed data and timetables 

# 1. BODS — Bus Open Data Service for all English bus timetables in GTFS format.
# London - Download timetables data in GTFS format
# https://data.bus-data.dft.gov.uk/timetable/download/

# 2. Tube, Overground, Elizabeth line, DLR 
# https://www.transit.land/feeds/f-transport~for~london

# 3. National Rail 
# Download and filter national rail feeds 

import requests
import os

# Credentials
EMAIL = "ucfnbte@ucl.ac.uk"
PASSWORD = "Jeanette89334837!"  

# ── Step 1: Authenticate and get token ──────────────────────────────────────

auth_url = "https://opendata.nationalrail.co.uk/authenticate"

auth_response = requests.post(
    auth_url,
    headers={"Content-Type": "application/x-www-form-urlencoded"},
    data={"username": EMAIL, "password": PASSWORD}
)

if auth_response.status_code != 200:
    print(f"✗ Authentication failed: HTTP {auth_response.status_code}")
    print(auth_response.text)
    exit(1)

token = auth_response.json()["token"]
print(f"✓ Authenticated — token: {token[:12]}...")

# ── Step 2: Download timetable feed ─────────────────────────────────────────

os.makedirs("large", exist_ok=True)
output_path = "large/national_rail_timetable.zip"

download_response = requests.get(
    "https://opendata.nationalrail.co.uk/api/staticfeeds/3.0/timetable",
    headers={
        "Content-Type": "application/json",
        "Accept": "*/*",
        "X-Auth-Token": token
    },
    stream=True
)

if download_response.status_code == 200:
    with open(output_path, "wb") as f:
        for chunk in download_response.iter_content(chunk_size=8192):
            f.write(chunk)
    size_mb = os.path.getsize(output_path) / 1_000_000
    print(f"✓ Saved to {output_path} ({size_mb:.1f} MB)")
else:
    print(f"✗ Download failed: HTTP {download_response.status_code}")
    print(download_response.text)

import zipfile, csv, io, os, shutil

# Agencies in national_rail_gtfs.zip that duplicate the ITM feed
EXCLUDE_AGENCY_IDS = {
    "LT",   # London Underground (already in ITM)
    "LO",   # London Overground (already in ITM)
    "XR",   # TfL Rail (already in ITM)
    # Network Rail infrastructure/freight - not passenger services
    "QA","QB","QC","QD","QE","QF","QG","QH","QJ","QK",
    "QL","QM","QN","QO","QP","QQ","QR","QS","QT","QU",
    "QV","QW","QY","QZ","LR","ZZ",
}

input_zip  = "large/national_rail_gtfs.zip"
output_zip = "large/national_rail_filtered_gtfs.zip"
os.makedirs("network", exist_ok=True)

def filter_gtfs(input_path, output_path, exclude_agency_ids):
    with zipfile.ZipFile(input_path) as zin, \
         zipfile.ZipFile(output_path, "w", zipfile.ZIP_DEFLATED) as zout:

        # ── 1. Filter agency.txt ──────────────────────────────────────
        with zin.open("agency.txt") as f:
            reader = list(csv.DictReader(io.TextIOWrapper(f, encoding="utf-8-sig")))
        keep_agencies = {r["agency_id"] for r in reader
                         if r["agency_id"] not in exclude_agency_ids}
        out = io.StringIO()
        w = csv.DictWriter(out, fieldnames=reader[0].keys())
        w.writeheader()
        w.writerows(r for r in reader if r["agency_id"] in keep_agencies)
        zout.writestr("agency.txt", out.getvalue())
        print(f"agency.txt:  {len(keep_agencies)} agencies kept")

        # ── 2. Filter routes.txt ─────────────────────────────────────
        with zin.open("routes.txt") as f:
            reader = list(csv.DictReader(io.TextIOWrapper(f, encoding="utf-8-sig")))
        keep_routes = {r["route_id"] for r in reader
                       if r["agency_id"] in keep_agencies}
        out = io.StringIO()
        w = csv.DictWriter(out, fieldnames=reader[0].keys())
        w.writeheader()
        w.writerows(r for r in reader if r["route_id"] in keep_routes)
        zout.writestr("routes.txt", out.getvalue())
        print(f"routes.txt:  {len(keep_routes)} routes kept")

        # ── 3. Filter trips.txt ──────────────────────────────────────
        with zin.open("trips.txt") as f:
            reader = list(csv.DictReader(io.TextIOWrapper(f, encoding="utf-8-sig")))
        keep_trips = {r["trip_id"] for r in reader
                      if r["route_id"] in keep_routes}
        out = io.StringIO()
        w = csv.DictWriter(out, fieldnames=reader[0].keys())
        w.writeheader()
        w.writerows(r for r in reader if r["trip_id"] in keep_trips)
        zout.writestr("trips.txt", out.getvalue())
        print(f"trips.txt:   {len(keep_trips)} trips kept")

        # ── 4. Filter stop_times.txt (streaming — large file) ────────
        with zin.open("stop_times.txt") as f:
            reader = csv.DictReader(io.TextIOWrapper(f, encoding="utf-8-sig"))
            out = io.StringIO()
            w = csv.DictWriter(out, fieldnames=reader.fieldnames)
            w.writeheader()
            kept = 0
            for row in reader:
                if row["trip_id"] in keep_trips:
                    w.writerow(row)
                    kept += 1
        zout.writestr("stop_times.txt", out.getvalue())
        print(f"stop_times.txt: {kept:,} rows kept")

        # ── 5. Pass through remaining files unchanged ─────────────────
        for name in ["stops.txt", "calendar.txt", "calendar_dates.txt", "transfers.txt"]:
            if name in zin.namelist():
                zout.writestr(name, zin.read(name))
                print(f"{name}: copied as-is")

filter_gtfs(input_zip, output_zip, EXCLUDE_AGENCY_IDS)
print(f"\n✓ Filtered feed saved to {output_zip}")

import zipfile, io, pandas as pd

SRC = "large/national_rail_filtered_gtfs.zip"
DST = "large/national_rail_fixed_gtfs.zip"

with zipfile.ZipFile(SRC) as zin:
    stops = pd.read_csv(zin.open("stops.txt"), dtype=str)
    valid_ids = set(stops["stop_id"].dropna())
    transfers = pd.read_csv(zin.open("transfers.txt"), dtype=str)

    before = len(transfers)
    transfers = transfers.dropna(subset=["from_stop_id", "to_stop_id"])
    transfers = transfers[
        transfers["from_stop_id"].isin(valid_ids) &
        transfers["to_stop_id"].isin(valid_ids)
    ]
    after = len(transfers)
    print(f"transfers: {before} → {after} (removed {before - after} bad rows)")

    with zipfile.ZipFile(DST, "w", compression=zipfile.ZIP_DEFLATED) as zout:
        for item in zin.infolist():
            if item.filename == "transfers.txt":
                buf = io.BytesIO()
                transfers.to_csv(buf, index=False)
                zout.writestr("transfers.txt", buf.getvalue())
            else:
                try:
                    data = zin.read(item.filename)
                    zout.writestr(item, data)
                except Exception as e:
                    print(f"  WARNING: skipping {item.filename} ({e})")

print(f"✓ Written to {DST}")

# Verify the output is readable
print("\nVerifying output zip...")
with zipfile.ZipFile(DST) as zcheck:
    names = zcheck.namelist()
    print(f"  Files in zip: {names}")
    for name in names:
        try:
            size = len(zcheck.read(name))
            print(f"  ✓ {name}: {size:,} bytes")
        except Exception as e:
            print(f"  ✗ {name}: {e}")