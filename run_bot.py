#!/usr/bin/env python3
import os
import sys

replit_domain = os.getenv("REPLIT_DEV_DOMAIN")
if replit_domain and not os.getenv("PUBLIC_URL"):
    os.environ["PUBLIC_URL"] = f"https://{replit_domain}"
    print(f"✅ Set PUBLIC_URL to: https://{replit_domain}")

import subprocess
subprocess.run([sys.executable, "-m", "uvicorn", "bot:app", "--host", "0.0.0.0", "--port", "5000"])
