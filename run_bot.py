#!/usr/bin/env python3
import os
import sys
import subprocess

replit_domain = os.getenv("REPLIT_DEV_DOMAIN")
if replit_domain and not os.getenv("PUBLIC_URL"):
    os.environ["PUBLIC_URL"] = f"https://{replit_domain}"
    print(f"✅ Set PUBLIC_URL to: https://{replit_domain}")

# === START DEBUGGING ===
print("="*30)
print("DEBUGGING ENVIRONMENT VARIABLES:")
ts_host = os.getenv("TYPESENSE_HOST")
ts_port = os.getenv("TYPESENSE_PORT")
ts_proto = os.getenv("TYPESENSE_PROTOCOL")
ts_key_exists = "SET" if os.getenv("TYPESENSE_API_KEY") else "NOT SET"

# Log values ko 'single quotes' mein print karein taaki extra space dikh jaaye
print(f"TYPESENSE_HOST: '{ts_host}'")
print(f"TYPESENSE_PORT: '{ts_port}'")
print(f"TYPESENSE_PROTOCOL: '{ts_proto}'")
print(f"TYPESENSE_API_KEY is: {ts_key_exists}")

if not ts_host:
    print("CRITICAL: TYPESENSE_HOST environment variable is NOT SET.")
else:
    # Aam galtiyon ko check karein
    if ' ' in ts_host:
        print("CRITICAL: TYPESENSE_HOST mein extra space hai!")
    if 'https://' in ts_host:
        print("CRITICAL: TYPESENSE_HOST mein 'https://' nahi hona chahiye!")
    if ':' in ts_host:
        print("CRITICAL: TYPESENSE_HOST mein port number (jaise ':443') nahi hona chahiye!")

print("="*30)
# === END DEBUGGING ===

# Render $PORT environment variable ka istemal karein.
# 5000 ko default fallback rakhein local testing ke liye.
port = os.getenv("PORT", "10000") # Render ke free tier ke liye 10000 set karein

print(f"✅ Starting Uvicorn on host 0.0.0.0, port {port}...")

# Uvicorn command ko port variable use karne ke liye update karein
subprocess.run([
    sys.executable,
    "-m", "uvicorn",
    "bot:app",
    "--host", "0.0.0.0",
    "--port", port
])
