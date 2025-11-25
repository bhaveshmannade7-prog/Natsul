# smart_watchdog.py
import asyncio
import logging
import os
import psutil # Naya dependency: Process monitoring
from datetime import datetime, timezone, timedelta
from typing import Dict, Any

# Local imports from your project
from aiogram import Bot
from aiogram.exceptions import TelegramAPIError, TelegramRetryAfter
from queue_wrapper import priority_queue, PRIORITY_ADMIN, QUEUE_CONCURRENCY
from bot import safe_tg_call 

logger = logging.getLogger("bot.watchdog")

# --- Configuration ---
ADMIN_ID = int(os.getenv("ADMIN_USER_ID", "7263519581")) # Using your provided ID as default
WATCHDOG_ENABLED = os.getenv("WATCHDOG_ENABLED", "True").lower() == 'true'
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
        self.owner_id = ADMIN_ID

    async def _send_alert(self, title: str, details: str):
        """Admin ko private DM mein alert bhejta hai।"""
        if self.owner_id == 0:
            logger.warning(f"Watchdog Alert (No Admin ID): {title}")
            return
            
        alert_message = (
            f"🐶 <b>SMART WATCHDOG ALERT: {title}</b>\n\n"
            f"<b>Details:</b> {details}\n"
            f"<b>Server Time:</b> {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}\n"
            f"<b>Uptime:</b> {(datetime.now(timezone.utc) - self.dp.start_time).total_seconds() // 60:.0f} minutes\n"
            f"---\n"
            f"<b>Recommended Action:</b> Kripya server logs aur DB connections check karein."
        )
        # Use safe_tg_call to prevent watchdog alert from causing more flood
        await safe_tg_call(
            self.bot.send_message(self.owner_id, alert_message),
            timeout=10 
        )
        logger.error(f"Watchdog Alert Sent: {title}")

    async def _monitor_cpu_and_freeze(self):
        """CPU high load aur Event Loop freeze ko monitor karta hai।"""
        try:
            # CPU utilization (non-blocking call, can take time)
            cpu_percent = psutil.cpu_percent(interval=None) 
            self.last_cpu_percent = cpu_percent

            # 1. CPU High Load Alert (Predict freeze risk)
            if cpu_percent >= CPU_ALERT_THRESHOLD:
                await self._send_alert(
                    "🔴 HIGH CPU LOAD PREDICTION",
                    f"Current CPU usage: {cpu_percent:.2f}%. Load {CPU_ALERT_THRESHOLD}% threshold se upar hai. Workers/Threads kaafi busy hain."
                )

            # 2. Queue Stuck / Worker Freeze Detect (Queue stuck / worker freeze detect kare)
            queue_size = priority_queue._queue.qsize()
            
            if queue_size > 0:
                # Non-blocking way to check head of queue
                if not priority_queue._queue.empty():
                    # PriorityQueue stores item: (priority, timestamp, update, ...)
                    _, timestamp, _, *rest = priority_queue._queue._queue[0]
                    stuck_duration = (datetime.now(timezone.utc) - timestamp).total_seconds()
                    
                    if stuck_duration >= QUEUE_STUCK_THRESHOLD:
                         self.freeze_prediction_count += 1
                         
                         if self.freeze_prediction_count >= 2: # 2 consecutive checks
                             await self._send_alert(
                                "🚨 WORKER/QUEUE FREEZE DETECTED",
                                f"High queue size ({queue_size}) aur oldest request {stuck_duration:.1f}s se stuck hai. Workers freeze ho sakte hain."
                            )
                             self.freeze_prediction_count = 0
                    else:
                         self.freeze_prediction_count = 0
            
        except Exception as e:
            logger.error(f"CPU Monitor failed: {e}", exc_info=False)


    async def _monitor_db_health(self):
        """DB connectivity aur overload status check karta hai।"""
        
        # 1. MongoDB Health Check (Low latency ping)
        mongo_ready = await self.db_primary.is_ready() # Direct call, safe_db_call is not needed here
        if not mongo_ready:
             await self._send_alert(
                 "❌ MONGO DB OFFLINE/DISCONNECTED",
                 "Primary MongoDB cluster se connection toot gaya hai. Critical services fail ho sakte hain."
             )
             
        # 2. NeonDB Health Check
        neon_ready = await self.db_neon.is_ready()
        if not neon_ready:
             await self._send_alert(
                 "❌ NEON DB OFFLINE/DISCONNECTED",
                 "Neon PostgreSQL cluster se connection toot gaya hai. File index/backup fail ho rahe honge."
             )
             
        # 3. Redis Health Check (Redis down/slow ho to alert bheje)
        redis_ready = self.redis_cache.is_ready()
        if self.redis_cache.redis and not redis_ready:
            await self._send_alert(
                "⚠️ REDIS OFFLINE/SLOW",
                "Redis Cache Node unavailable ya bahut slow hai. Throttling load ab MongoDB par jayega (CPU risk)."
            )

    async def _monitor_tg_and_webhook(self):
        """Telegram flood lock, token status aur webhook inactivity check karta hai।"""
        
        # 1. Check Token Rotation Fail / Flood-Lock Detect
        try:
            # Low-cost Telegram operation (getting chat info for admin is safe)
            await safe_tg_call(
                self.bot.get_chat(chat_id=self.owner_id),
                timeout=5
            )
            
            # Webhook Inactivity/Delay (Simple check)
            current_webhook = await safe_tg_call(self.bot.get_webhook_info())
            if current_webhook and current_webhook.pending_update_count > 100:
                 await self._send_alert(
                    "⚠️ WEBHOOK BACKLOG/DELAY",
                    f"Telegram par {current_webhook.pending_update_count} updates pending hain. Webhook process hone mein deri ho rahi hai."
                 )
                 
        except TelegramRetryAfter as e:
            await self._send_alert(
                "🚫 TELEGRAM FLOOD LOCK",
                f"Bot ko Telegram ne flood-lock mein daal diya hai ({e.retry_after}s). Requests discard ho rahi hongi. (Token Rotation Fail/flood-lock detect kare)"
            )
        except TelegramAPIError as e:
             error_msg = str(e)
             # Token rotation fail/flood-lock detect kare
             if "Unauthorized" in error_msg or "Invalid token" in error_msg:
                 await self._send_alert(
                     "🔑 BOT TOKEN AUTH FAILURE",
                     "Bot ka main token invalid ya expired ho gaya hai. Kripya .env mein BOT_TOKEN update karein."
                 )
             else:
                 logger.error(f"TG Monitor API Error: {e}", exc_info=False)
        except Exception as e:
             logger.error(f"TG Monitor failed: {e}", exc_info=False)


    async def run_watchdog(self):
        """Main periodic watchdog loop।"""
        self.is_running = True
        logger.info(f"Smart Watchdog started (Interval: {CHECK_INTERVAL}s).")
        
        # Small initial delay to let DB connections stabilize
        await asyncio.sleep(5) 
        
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
        if WATCHDOG_ENABLED and not self.is_running and self.owner_id != 0:
            # Rule: background me chalega.
            self.task = asyncio.create_task(self.run_watchdog())
            return True
        return False
        
    def stop(self):
        """Watchdog ko band karta hai।"""
        if self.task:
            self.task.cancel()
            self.is_running = False
            logger.info("Smart Watchdog stopped.")
