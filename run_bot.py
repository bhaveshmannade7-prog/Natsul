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

# --- Sabhi critical variables ko check karein ---
config = {
    "BOT_TOKEN": os.getenv("BOT_TOKEN"),
    "ADMIN_USER_ID": os.getenv("ADMIN_USER_ID"),
    "DATABASE_URL": os.getenv("DATABASE_URL"),
    "NEON_DATABASE_URL": os.getenv("NEON_DATABASE_URL"),
    "TYPESENSE_HOST": os.getenv("TYPESENSE_HOST"),
    "TYPESENSE_PORT": os.getenv("TYPESENSE_PORT"),
    "TYPESENSE_PROTOCOL": os.getenv("TYPESENSE_PROTOCOL"),
    "TYPESENSE_API_KEY": os.getenv("TYPESENSE_API_KEY")
}

all_set = True
for key, value in config.items():
    if value:
        # Passwords/keys ko log mein na dikhayein
        if "TOKEN" in key or "KEY" in key or "URL" in key:
            print(f"✅ {key}: SET (Hidden)")
        else:
            print(f"✅ {key}: '{value}'")
    else:
        print(f"❌ CRITICAL: {key} environment variable is NOT SET.")
        all_set = False

print("="*30)
# === END DEBUGGING ===

# Render $PORT environment variable ka istemal karein.
port = os.getenv("PORT", "10000")

print(f"✅ Starting Uvicorn on host 0.0.0.0, port {port}...")

# Uvicorn command
subprocess.run([
    sys.executable,
    "-m", "uvicorn",
    "bot:app",
    "--host", "0.0.0.0",
    "--port", port
])
