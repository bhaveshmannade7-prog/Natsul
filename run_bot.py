#!/usr/bin/env python3
import os
import sys
import subprocess

replit_domain = os.getenv("REPLIT_DEV_DOMAIN")
if replit_domain and not os.getenv("PUBLIC_URL"):
    os.environ["PUBLIC_URL"] = f"https://{replit_domain}"
    print(f"✅ Set PUBLIC_URL to: https://{replit_domain}")

# Render $PORT environment variable ka istemal karein.
# 5000 ko default fallback rakhein local testing ke liye.
port = os.getenv("PORT", "5000")

print(f"✅ Starting Uvicorn on host 0.0.0.0, port {port}...")

# Uvicorn command ko port variable use karne ke liye update karein
subprocess.run([
    sys.executable,
    "-m", "uvicorn",
    "bot:app",
    "--host", "0.0.0.0",
    "--port", port
])
