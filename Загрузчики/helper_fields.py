#!/usr/bin/env python3
import os, requests, json
from dotenv import load_dotenv

load_dotenv()  

DOMAIN   = "rentanalyticstest.amocrm.ru"
TOKEN = os.getenv("AMO_TOKEN")
HEAD   = {"Authorization": f"Bearer {TOKEN}",
          "Content-Type": "application/json"}
BASE   = f"https://{DOMAIN}"

FIELDS = [
    {"name": "Plate",        "type": "text"},
    {"name": "Car model",    "type": "text"},
    {"name": "Daily rate",   "type": "numeric"},
    {"name": "Client name",  "type": "text"},
    {"name": "Phone",        "type": "text"},
    {"name": "Email",        "type": "text"},
    {"name": "Rental start", "type": "date"},
    {"name": "Rental end",   "type": "date"}
]

r = requests.post(f"{BASE}/api/v4/leads/custom_fields",
                  headers=HEAD, json=FIELDS, timeout=30)
print(r.status_code)

