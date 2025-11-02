#!/usr/bin/env python3
"""
rental_loader.py   –   Full demo filler for Car Rental pipeline
────────────────────────────────────────────────────────────────
Requirements:
  pip install requests python-dotenv
.env variables:
  AMO_TOKEN       – long-lived token
  AMO_SUBDOMAIN   – rentanalyticstest
"""

import os, sys, random, datetime as dt, math, requests, itertools
from dotenv import load_dotenv

# ────────────────────────── ENV ────────────────────────────
load_dotenv()
TOKEN   = os.getenv("AMO_TOKEN")
DOMAIN  = os.getenv("AMO_SUBDOMAIN", "rentanalyticstest")
if not TOKEN:
    sys.exit("❌  AMO_TOKEN missing in .env")

BASE = f"https://{DOMAIN}.amocrm.ru"
HEAD = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}

# ──────────────────────── CONSTANTS ───────────────────────
PIPELINE_ID = 9877710
STATUS = {
    "Inquiry":   78530694,
    "Quote":     78530698,
    "Deposit":   78530702,
    "Confirmed": 78530706,
    "OnRent":    78530710,
    "Returned":  78530714,
    "Cancelled": 78530722
}
CARS_CATALOG_ID, CLIENTS_CATALOG_ID = 8525, 8527
REQUIRED_FIELDS = ["MODEL","PLATE","DAILYRATE","CLIENT NAME","LICENSE",
                   "Rental start","Rental end"]

STAGE_COUNTS = { "Inquiry":15,"Quote":12,"Deposit":12,
                 "Confirmed":20,"OnRent":6,"Returned":25,"Cancelled":16 }

MONTHS = [6,7,8]       # June–August 2025
MAX_BATCH   = 49
WIPE_FIRST  = True
random.seed(42)

# Get today's date
TODAY = dt.date.today()

# ──────────────────────── HTTP helpers ─────────────────────
def api(method, path, **kw):
    url = f"{BASE}{path}"
    r   = requests.request(method, url, headers=HEAD, timeout=90, **kw)
    r.raise_for_status()
    return r.json() if r.text else {}

# ───────────── ensure / fetch LEAD custom-fields ───────────
def ensure_fields():
    current = {f["name"]: f["id"]
               for f in api("GET","/api/v4/leads/custom_fields?limit=250")
                 ["_embedded"]["custom_fields"]}
    missing = [n for n in REQUIRED_FIELDS if n not in current]
    body = [{"name": n, "type":"text"} for n in missing
            if n not in ("Rental start","Rental end")]
    if body:
        resp = api("POST","/api/v4/leads/custom_fields", json=body)
        for f in resp["_embedded"]["custom_fields"]:
            current[f["name"]] = f["id"]
    return current
FIELD_ID = ensure_fields()
print("✔ Lead fields:", FIELD_ID)

# ───────────── optional WIPE pipeline ───────────────────────
if WIPE_FIRST:
    print("🗑️  Wiping pipeline …")
    page=1
    while True:
        chunk = api("GET","/api/v4/leads",
                    params={"pipeline_id":PIPELINE_ID,"page":page,"limit":250})
        leads = chunk.get("_embedded",{}).get("leads",[])
        if not leads: break
        ids = ",".join(str(l["id"]) for l in leads)
        api("DELETE","/api/v4/leads", params={"ids":ids})
        page+=1
    print("✓  Cleared.")

# ──────────── cache catalogs Cars & Clients ─────────────────
def fetch_cat(cid):
    res, page = {}, 1
    while True:
        chunk = api("GET", f"/api/v4/catalogs/{cid}/elements",
                    params={"page":page,"limit":250})
        for el in chunk["_embedded"]["elements"]:
            res[el["name"]] = el
        if len(chunk["_embedded"]["elements"])<250: break
        page+=1
    return res

def cf_map(el):
    m={}
    for f in el.get("custom_fields_values", []):
        key = f.get("code") or f.get("field_name") or f["field_id"]
        m[key] = f["values"][0]["value"]
    return m

print("📥  Caching catalogs …")
cars_raw    = fetch_cat(CARS_CATALOG_ID)
clients_raw = fetch_cat(CLIENTS_CATALOG_ID)

cars={}
for key, el in cars_raw.items():
    cf = cf_map(el)
    cars[key] = {
        "id":    el["id"],
        "model": cf.get("MODEL", key),
        "plate": cf.get("PLATE", key),
        "rate":  float(cf.get("DAILYRATE", 1000))
    }

clients={}
for key, el in clients_raw.items():
    cf = cf_map(el)
    clients[key] = {
        "id":   el["id"],
        "name": cf.get("ClientName", key),
        "lic":  cf.get("License", "")
    }

# ─────────── booking algorithm (weighted) ───────────────────
booked = {p: [] for p in cars}
ALPHA=1.2
CAR_LIST = list(cars)
WEIGHTS  = [1/math.pow(i+1,ALPHA) for i in range(len(CAR_LIST))]

def rand_slot(stage):
    """Generate rental slot based on stage requirements"""
    # For OnRent: dates should include today
    if stage == "OnRent":
        # Start date should be before today, end date should be after today
        days_before = random.randint(1, 5)
        days_after = random.randint(1, 5)
        s = TODAY - dt.timedelta(days=days_before)
        e = TODAY + dt.timedelta(days=days_after)
    
    # For Returned/Cancelled: end date should be before today
    elif stage in ("Returned", "Cancelled"):
        days_ago_end = random.randint(1, 30)
        days_rental = random.randint(1, 6)
        e = TODAY - dt.timedelta(days=days_ago_end)
        s = e - dt.timedelta(days=days_rental)
    
    # For all others: start date should be after today
    else:
        days_ahead = random.randint(1, 60)
        days_rental = random.randint(1, 6)
        s = TODAY + dt.timedelta(days=days_ahead)
        e = s + dt.timedelta(days=days_rental)
    
    # Ensure dates are within our months range
    year = 2025
    if s.month in MONTHS and s.year == year:
        pass
    else:
        # Adjust to fit within our months
        m = random.choice(MONTHS)
        if stage == "OnRent":
            # For current rentals, use current year and adjust around today
            s = dt.date(TODAY.year, TODAY.month, max(1, min(28, TODAY.day - random.randint(1, 5))))
            e = dt.date(TODAY.year, TODAY.month, max(1, min(28, TODAY.day + random.randint(1, 5))))
        elif stage in ("Returned", "Cancelled"):
            # For past rentals
            s = dt.date(year, m, random.randint(1, 20))
            e = s + dt.timedelta(days=random.randint(1, 6))
            # Make sure it's in the past
            if e >= TODAY:
                days_shift = (e - TODAY).days + random.randint(1, 10)
                e = e - dt.timedelta(days=days_shift)
                s = s - dt.timedelta(days=days_shift)
        else:
            # For future rentals
            s = dt.date(year, m, random.randint(1, 22))
            e = s + dt.timedelta(days=random.randint(1, 6))
    
    iso = lambda d:f"{d}T00:00:00+00:00"
    return s, e, iso(s), iso(e)

def allocate(stage):
    for _ in range(6000):
        plate_key = random.choices(CAR_LIST, weights=WEIGHTS, k=1)[0]
        for _ in range(400):
            s,e,s_iso,e_iso = rand_slot(stage)
            if any(not (e<s2 or s>e2) for s2,e2 in booked[plate_key]):
                continue
            booked[plate_key].append((s,e))
            days = (e-s).days or 1
            return plate_key,s_iso,e_iso,days
    raise RuntimeError("Extend date range or add cars.")

# ─────────── build lead payloads ────────────────────────────
payload=[]

def add(stage):
    plate_key,s_iso,e_iso,days=allocate(stage)
    car   = cars[plate_key]
    cli   = clients[str(random.randint(1,30))]

    cfv = [
        {"field_id": FIELD_ID["MODEL"],       "values":[{"value": car["model"]}]},
        {"field_id": FIELD_ID["PLATE"],       "values":[{"value": car["plate"]}]},
        {"field_id": FIELD_ID["CLIENT NAME"], "values":[{"value": cli["name"]}]},
        {"field_id": FIELD_ID["LICENSE"],     "values":[{"value": cli["lic"]}]}
    ]
    if stage in ("Deposit","Confirmed","OnRent","Returned"):
        cfv += [
          {"field_id": FIELD_ID["DAILYRATE"],    "values":[{"value": str(car["rate"])}]},
          {"field_id": FIELD_ID["Rental start"], "values":[{"value": s_iso}]},
          {"field_id": FIELD_ID["Rental end"],   "values":[{"value": e_iso}]}
        ]

    price = int(car["rate"] * days) if stage=="Confirmed" else 0
    payload.append({
        "name": f"{car['plate']} · {cli['name']}",
        "pipeline_id": PIPELINE_ID,
        "status_id": STATUS[stage],
        "price": price,
        "custom_fields_values": cfv,
        "_embedded": {
            "catalog_elements":[
                {"catalog_id":CARS_CATALOG_ID,   "id": car["id"]},
                {"catalog_id":CLIENTS_CATALOG_ID,"id": cli["id"]}
            ]
        }
    })

for stage,count in STAGE_COUNTS.items():
    for _ in range(count):
        add(stage)

# ─────────── upload in batches ──────────────────────────────
print(f"🚀  Uploading {len(payload)} leads …")
for i in range(0,len(payload),MAX_BATCH):
    r = requests.post(f"{BASE}/api/v4/leads/complex", headers=HEAD,
                      json=payload[i:i+MAX_BATCH], timeout=90)
    print(f"Batch {i//MAX_BATCH+1}: {r.status_code}")
    if r.status_code>=300:
        print(r.text[:400]); sys.exit(1)

print("✅  Done – pipeline populated.")