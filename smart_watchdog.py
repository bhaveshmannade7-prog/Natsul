# smart_watchdog.py
import asyncio
import logging
import os
from datetime import datetime, timezone, timedelta
from typing import Dict, Any
import psutil # Naya dependency: Process monitoring

# Local imports from your project
from aiogram import Bot
from aiogram.exceptions import TelegramAPIError
from queue_wrapper import priority_queue, PRIORITY_ADMIN, QUEUE_CONCURRENCY
from bot import safe_tg_call 

logger = logging.getLogger("bot.watchdog")

# --- Configuration ---
ADMIN_ID = int(os.getenv("ADMIN_USER_ID", "0"))
WATCHDOG_ENABLED = os.getenv("WATCHDOG_ENABLED", "False").lower() == 'true'
CHECK_INTERVAL = int(os.getenv("WATCHDOG_INTERVAL", "60")) # 60 seconds
CPU_ALERT_THRESHOLD = 80.0 # CPU usage over 80%
QUEUE_STUCK_THRESHOLD = 30 # Queue stuck for 30 seconds

class SmartWatchdog:
    def __init__(self, bot_instance: Bot, dp_instance: Any, db_objects: Dict[str, Any]):
        self.bot = bot_instance
        self.dp = dp_instance
        self.db_primary = db_objects['db_primary']
        self.db_neon = db_objects['db_neon']
        self.redis_cache = db_objects['redis_cache']
        self.last_check_time = datetime.now(timezone.utc)
        self.last_cpu_percent = 0.0
        self.freeze_prediction_count = 0
        self.is_running = False
        self.task: asyncio.Task | None = None
        self.last_webhook_check_time = datetime.now(timezone.utc)

    async def _send_alert(self, title: str, details: str):
        """Admin ko private DM mein alert bhejta hai।"""
        if ADMIN_ID == 0:
            logger.warning(f"Watchdog Alert (No Admin ID): {title}")
            return
            
        alert_message = (
            f"🐶 <b>SMART WATCHDOG ALERT: {title}</b>\n\n"
            f"<b>Details:</b> {details}\n"
            f"<b>Server Time:</b> {datetime.now(timezone.utc).strftime('%H:%M:%S UTC')}\n"
            f"---\n"
            f"<b>Recommended Action:</b> Kripya server logs aur DB connections check karein."
        )
        # Use safe_tg_call to prevent watchdog alert from causing more flood
        await safe_tg_call(
            self.bot.send_message(ADMIN_ID, alert_message),
            timeout=10 
        )
        logger.error(f"Watchdog Alert Sent: {title}")

    async def _monitor_cpu_and_freeze(self):
        """CPU high load aur Event Loop freeze ko monitor karta hai।"""
        # Rule: CPU usage reduction strategies
        try:
            # CPU utilization (non-blocking call, can take time)
            cpu_percent = psutil.cpu_percent(interval=None) 
            self.last_cpu_percent = cpu_percent

            # 1. CPU High Load Alert
            if cpu_percent >= CPU_ALERT_THRESHOLD:
                await self._send_alert(
                    "🔴 HIGH CPU LOAD PREDICTION",
                    f"Current CPU usage: {cpu_percent:.2f}%. Load {CPU_ALERT_THRESHOLD}% threshold se upar hai."
                )

            # 2. Queue Stuck / Worker Freeze Detection
            queue_size = priority_queue._queue.qsize()
            
            if queue_size > 0:
                # Check if the oldest item is stuck (PriorityQueue stores item + timestamp)
                # Note: Queue item is (priority, timestamp, update, ...)
                
                # Non-blocking way to check head of queue
                if not priority_queue._queue.empty():
                    priority, timestamp, update, *rest = priority_queue._queue._queue[0]
                    stuck_duration = (datetime.now(timezone.utc) - timestamp).total_seconds()
                    
                    if stuck_duration >= QUEUE_STUCK_THRESHOLD:
                         self.freeze_prediction_count += 1
                         
                         if self.freeze_prediction_count >= 2: # 2 consecutive checks
                             await self._send_alert(
                                "🚨 WORKER/QUEUE FREEZE DETECTED",
                                f"Queue stuck hai. Size: {queue_size}. Pehli request {stuck_duration:.1f}s se stuck hai. Workers free ho sakte hain."
                            )
                             self.freeze_prediction_count = 0
                    else:
                         self.freeze_prediction_count = 0
            
        except Exception as e:
            logger.error(f"CPU Monitor failed: {e}", exc_info=False)


    async def _monitor_db_health(self):
        """DB connectivity aur overload status check karta hai।"""
        # Rule: Least expensive DB access patterns
        
        # 1. MongoDB Health Check (Low latency ping)
        mongo_ready = await safe_db_call(self.db_primary.is_ready(), default=False)
        if not mongo_ready:
             await self._send_alert(
                 "❌ MONGO DB OFFLINE/DISCONNECTED",
                 "Primary MongoDB cluster se connection toot gaya hai. Fallback Node (M2) par asar padh sakta hai."
             )
             
        # 2. Redis Health Check
        redis_ready = self.redis_cache.is_ready()
        if self.redis_cache.redis and not redis_ready:
            await self._send_alert(
                "⚠️ REDIS OFFLINE/SLOW",
                "Redis Cache Node unavailable ya bahut slow hai. Throttling aur concurrency check load ab MongoDB par jayega."
            )

    async def _monitor_tg_and_webhook(self):
        """Telegram flood lock aur webhook status check karta hai।"""
        
        # 1. Check Token Lock/Flood Status
        # Hum safe_tg_call ki functionality ko reverse use kar sakte hain
        try:
            # Trying a low-cost Telegram operation (like getting chat info for a known chat)
            # We use the main bot instance here
            await safe_tg_call(
                self.bot.get_chat(chat_id=ADMIN_ID),
                timeout=5
            )
        except TelegramRetryAfter as e:
            await self._send_alert(
                "🚫 TELEGRAM FLOOD LOCK",
                f"Bot ko Telegram ne flood-lock mein daal diya hai ({e.retry_after}s). Requests discard ho rahi hongi."
            )
        except Exception as e:
             # Predict future error: Token rotation fail
             if "Invalid token" in str(e) or "Unauthorized" in str(e):
                 await self._send_alert(
                     "🔑 BOT TOKEN AUTH FAILURE",
                     "Bot ka main token invalid ya expired ho gaya hai. Kripya .env mein BOT_TOKEN update karein."
                 )
             pass


    async def run_watchdog(self):
        """Main periodic watchdog loop।"""
        if not WATCHDOG_ENABLED:
            logger.warning("Watchdog is disabled via WATCHDOG_ENABLED=False in .env.")
            return

        self.is_running = True
        logger.info(f"Smart Watchdog started (Interval: {CHECK_INTERVAL}s).")
        
        while self.is_running:
            try:
                # Run monitoring tasks concurrently to save time
                await asyncio.gather(
                    self._monitor_cpu_and_freeze(),
                    self._monitor_db_health(),
                    self._monitor_tg_and_webhook(),
                    return_exceptions=True
                )
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.critical(f"Watchdog main loop failed: {e}", exc_info=True)
            
            await asyncio.sleep(CHECK_INTERVAL)

    def start(self):
        """Watchdog ko shuru karta hai।"""
        if not self.is_running and ADMIN_ID != 0:
            self.task = asyncio.create_task(self.run_watchdog())
            return True
        return False
        
    def stop(self):
        """Watchdog ko band karta hai।"""
        if self.task:
            self.task.cancel()
            self.is_running = False
            logger.info("Smart Watchdog stopped.")

# Global instance will be created in bot.py
