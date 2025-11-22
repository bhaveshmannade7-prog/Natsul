# gunicorn_runner.py
import os
import sys
import logging

# Ensure the main application is imported for Gunicorn to find the 'app' object
# Yeh bot:app ke liye zaroori hai
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

try:
    # bot.py mein defined FastAPI 'app' object ko import karein
    from bot import app
    
    # Check karein ki app object sahi se import hua hai
    if 'app' not in locals() or app is None:
        raise ImportError("FastAPI 'app' object 'bot.py' se nahi mila.")
        
    logging.info("FastAPI application object 'app' successfully imported from bot.py.")
    
except ImportError as e:
    logging.critical(f"FATAL: Gunicorn entry point error: {e}")
    sys.exit(1)

# Gunicorn yahan se 'app' variable uthayega.
# Is file mein aur kuch nahi karna hai.
