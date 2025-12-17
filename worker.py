import os
import time
from datetime import datetime, timedelta
import pytz

# импортируй твой основной файл как модуль
# лучше вынести парсинг в функцию run() в main.py
from main import run  # <- ниже объясню, что поменять

TZ = pytz.timezone(os.getenv("TZ", "Asia/Almaty"))
RUN_HOUR = int(os.getenv("RUN_HOUR", "9"))
RUN_MINUTE = int(os.getenv("RUN_MINUTE", "0"))

def seconds_until_next_run():
    now = datetime.now(TZ)
    target = now.replace(hour=RUN_HOUR, minute=RUN_MINUTE, second=0, microsecond=0)
    if target <= now:
        target += timedelta(days=1)
    return (target - now).total_seconds()

while True:
    sleep_sec = seconds_until_next_run()
    print(f"[worker] sleeping {int(sleep_sec)}s until next run")
    time.sleep(sleep_sec)

    try:
        print("[worker] starting run()")
        run()  # запускает твой парсер + отправку в телеграм
        print("[worker] done")
    except Exception as e:
        print(f"[worker] ERROR: {e}", flush=True)
        # если хочешь — тут можно ещё отправить сообщение в телеграм об ошибке
