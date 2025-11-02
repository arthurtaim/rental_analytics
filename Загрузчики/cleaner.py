#!/usr/bin/env python3
"""
clean_lead_fields.py
────────────────────
  • Keeps exactly 7 custom fields in the LEADS entity.
  • Removes every other deletable field.
  • Skips system / undeletable tracking fields.

Requires .env  (AMO_TOKEN, AMO_SUBDOMAIN)
"""

import os, sys, requests, collections
from dotenv import load_dotenv      # pip install python-dotenv

load_dotenv()
TOKEN   = os.getenv("AMO_TOKEN")
DOMAIN  = os.getenv("AMO_SUBDOMAIN", "rentanalyticstest")
if not TOKEN:
    sys.exit("AMO_TOKEN not found in .env")

BASE = f"https://{DOMAIN}.amocrm.ru"
HEAD = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}

# --- what we KEEP --------------------------------------------------
KEEP = {
    "MODEL":        884297,
    "PLATE":        884295,
    "DAILYRATE":    884299,
    "CLIENT NAME":  884301,
    "LICENSE":      884307,
    "Rental start": 883843,
    "Rental end":   883845
}
# ------------------------------------------------------------------

def get_fields():
    r = requests.get(f"{BASE}/api/v4/leads/custom_fields",
                     headers=HEAD, timeout=30)
    r.raise_for_status()
    return r.json()["_embedded"]["custom_fields"]

print("🔍  Listing lead fields …")
fields = get_fields()

# 1️⃣  separate what to delete / what to keep duplicates of
to_delete = []

by_name = collections.defaultdict(list)
for f in fields:
    by_name[f["name"]].append(f)

for name, items in by_name.items():
    if name in KEEP:
        # leave the one whose id matches KEEP  → remove the rest (if deletable)
        for item in items:
            if item["id"] != KEEP[name] and item.get("is_deletable", True):
                to_delete.append(item)
    else:
        for item in items:
            if item.get("is_deletable", True):
                to_delete.append(item)

# 2️⃣  delete
if not to_delete:
    print("✓  Nothing to delete — only required fields present.")
    sys.exit()

for f in to_delete:
    fid, fname = f["id"], f["name"]
    resp = requests.delete(
        f"{BASE}/api/v4/leads/custom_fields/{fid}",
        headers=HEAD, timeout=30
    )
    if resp.status_code == 204:
        print(f"🗑️  Deleted  {fname}  (id {fid})")
    else:
        print(f"⚠️  Could not delete {fname} (id {fid}): {resp.text[:120]}")

print("✅  Cleanup finished.")
