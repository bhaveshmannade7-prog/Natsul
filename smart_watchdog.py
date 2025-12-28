# smart_watchdog.py
import asyncio
import logging
import os
import psutil # Resource monitoring
from datetime import datetime, timezone, timedelta
from typing import Dict, Any

# FIX: Circular import se bachne ke liye safe_tg_call ko core_utils se import karein
from core_utils import safe_tg_call 
from aiogram import Bot
from aiogram.exceptions import TelegramAPIError, TelegramRetryAfter
from queue_wrapper import priority_queue

logger = logging.getLogger("bot.watchdog")

# --- Configuration ---
ADMIN_ID = int(os.getenv("ADMIN_USER_ID", "7263519581"))
WATCHDOG_ENABLED = os.getenv("WATCHDOG_ENABLED", "True").lower() == 'true'
CHECK_INTERVAL = int(os.getenv("WATCHDOG_INTERVAL", "60")) # 60 seconds default
CPU_ALERT_THRESHOLD = 85.0 # Alert if CPU > 85%
RAM_ALERT_THRESHOLD = 85.0 # Alert if RAM > 85% (Critical for Free Tier)
QUEUE_STUCK_THRESHOLD = 45 # Alert if a task is stuck for > 45s

class SmartWatchdog:
    def __init__(self, bot_instance: Bot, dp_instance: Any, db_objects: Dict[str, Any]):
        self.bot = bot_instance
        self.dp = dp_instance
        self.db_primary = db_objects['db_primary']
        self.db_neon = db_objects['db_neon']
        self.redis_cache = db_objects['redis_cache']
        
        self.owner_id = ADMIN_ID
        self.is_running = False
        self.task: asyncio.Task | None = None
        
        # Smart Alert Throttling (To prevent spamming admin)
        self.alert_history = {} 
        self.ALERT_COOLDOWN = 900 # 15 Minutes cooldown per alert type

    async def _send_alert(self, alert_key: str, title: str, details: str):
        """Sends alert to Admin with throttling logic."""
        if self.owner_id == 0: return

        # Throttling Check: Agar abhi haal hi mein ye alert bheja tha, to skip karo
        last_sent = self.alert_history.get(alert_key)
        now = datetime.now(timezone.utc)
        if last_sent and (now - last_sent).total_seconds() < self.ALERT_COOLDOWN:
            logger.warning(f"Watchdog Alert Suppressed (Cooldown): {title}")
            return

        # Update last sent time
        self.alert_history[alert_key] = now
        
        uptime_seconds = (now - self.dp.start_time).total_seconds()
        uptime_str = f"{int(uptime_seconds // 3600)}h {int((uptime_seconds % 3600) // 60)}m"

        alert_message = (
            f"🐶 <b>SMART WATCHDOG ALERT</b>\n"
            f"⚠️ <b>{title}</b>\n\n"
            f"📝 <b>Details:</b> {details}\n"
            f"🕒 <b>Time:</b> {now.strftime('%H:%M:%S UTC')}\n"
            f"⏳ <b>Uptime:</b> {uptime_str}\n"
            f"🛡️ <i>System is monitoring...</i>"
        )
        
        # Fire and forget (safe call)
        asyncio.create_task(safe_tg_call(
            self.bot.send_message(self.owner_id, alert_message),
            timeout=10
        ))
        logger.error(f"Watchdog Alert Sent: {title}")

    async def _monitor_resources(self):
        """Monitors CPU, RAM, and Disk Usage."""
        try:
            # 1. CPU Check
            cpu = psutil.cpu_percent(interval=None)
            if cpu >= CPU_ALERT_THRESHOLD:
                await self._send_alert("high_cpu", "🔥 HIGH CPU LOAD", f"CPU Usage is at {cpu}%. Workers might be overloaded.")

            # 2. RAM Check (Critical for Render/AWS Free Tier)
            ram = psutil.virtual_memory()
            if ram.percent >= RAM_ALERT_THRESHOLD:
                used_mb = ram.used // 1024 // 1024
                await self._send_alert(
                    "high_ram", 
                    "💾 HIGH RAM USAGE (OOM RISK)", 
                    f"RAM Usage: {ram.percent}% ({used_mb}MB used). Bot crash risk high!"
                )

            # 3. Disk Check (Logs filling up)
            disk = psutil.disk_usage('.')
            if disk.percent >= 90:
                await self._send_alert("high_disk", "💿 LOW DISK SPACE", f"Disk usage is at {disk.percent}%. Cleanup required.")

        except Exception as e:
            logger.error(f"Resource monitor error: {e}")

    async def _monitor_queue_health(self):
        """Checks for frozen workers or stuck queue items."""
        try:
            # Access the underlying PriorityQueue instance
            queue_instance = priority_queue._queue
            queue_size = queue_instance.qsize()
            
            # Safe Queue Peek
            if queue_size > 0:
                try:
                    # Access internal deque/list safely to see the oldest item
                    # Internal structure: _queue._queue is a list (heap)
                    internal_queue = queue_instance._queue
                    if internal_queue:
                        # Item structure: (priority, timestamp, update, bot, db_objects)
                        # We index [0] because it's a heap (smallest item = highest priority/oldest)
                        oldest_item = internal_queue[0]
                        timestamp = oldest_item[1]
                        
                        stuck_duration = (datetime.now(timezone.utc) - timestamp).total_seconds()
                        
                        if stuck_duration > QUEUE_STUCK_THRESHOLD:
                            await self._send_alert(
                                "queue_stuck", 
                                "🧊 WORKER FREEZE / QUEUE STUCK", 
                                f"Queue has {queue_size} items pending.\nOldest task stuck for {stuck_duration:.1f}s.\nWorkers might be dead."
                            )
                except IndexError:
                    pass # Queue emptied while checking
                except Exception as qe:
                    logger.warning(f"Could not peek queue internals (structure changed?): {qe}")
        except Exception as e:
            logger.error(f"Queue monitor error: {e}")

    async def _monitor_services(self):
        """Checks Databases and Redis Connectivity."""
        # 1. MongoDB
        if not await self.db_primary.is_ready():
            await self._send_alert("mongo_down", "❌ MONGODB PRIMARY DOWN", "Connection to MongoDB Atlas failed. Bot functionality is limited.")

        # 2. NeonDB
        if not await self.db_neon.is_ready():
            await self._send_alert("neon_down", "❌ NEON DB DOWN", "Connection to Neon PostgreSQL failed. Search/Backup affected.")

        # 3. Redis
        if self.redis_cache.redis and not self.redis_cache.is_ready():
            await self._send_alert("redis_down", "⚠️ REDIS DOWN", "Redis Cache is unreachable. System falling back to DB (Slower).")

    async def run_watchdog(self):
        """Main Watchdog Loop"""
        if not WATCHDOG_ENABLED:
            return
        
        logger.info(f"Smart Watchdog Active (Interval: {CHECK_INTERVAL}s)")
        
        # Initial Warmup Delay (Let bot start fully)
        await asyncio.sleep(15)

        while self.is_running:
            try:
                # Run all checks in parallel to save time
                await asyncio.gather(
                    self._monitor_resources(),
                    self._monitor_queue_health(),
                    self._monitor_services(),
                    return_exceptions=True
                )
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.critical(f"Watchdog Loop Crash: {e}", exc_info=True)
                await asyncio.sleep(5) # Short sleep on crash
            
            await asyncio.sleep(CHECK_INTERVAL)

    def start(self):
        if WATCHDOG_ENABLED and not self.is_running:
            self.is_running = True
            self.task = asyncio.create_task(self.run_watchdog())
            logger.info("Watchdog task started.")

    def stop(self):
        if self.task:
            self.task.cancel()
            self.is_running = False
            logger.info("Watchdog task stopped.")
