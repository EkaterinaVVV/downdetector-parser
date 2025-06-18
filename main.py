import requests
import pandas as pd
import re
from datetime import datetime

def parse_from_data_js(service_rus_name: str) -> pd.DataFrame | None:
    url = f"https://downdetector.info/data.js?service={service_rus_name}"
    headers = {"User-Agent": "Mozilla/5.0"}

    try:
        r = requests.get(url, headers=headers)
        r.encoding = 'utf-8'  # Учитываем кириллицу

        if r.status_code != 200:
            print(f"[Ошибка] Код ответа: {r.status_code}")
            return None

        text = r.text

        # Находим массивы в тексте JavaScript
        hour_match = re.search(r"var hourlabels = (\[.*?\]);", text)
        data_match = re.search(r"var datavalues = (\[.*?\]);", text)

        if not hour_match or not data_match:
            print("[Ошибка] Структура JS изменилась — данные не найдены.")
            return None

        hours = eval(hour_match.group(1))
        values = eval(data_match.group(1))

        today = datetime.now().strftime("%Y-%m-%d")
        df = pd.DataFrame({
            "datetime": [f"{today} {h}:00" for h in hours],
            "num_reports": values,
            "service": service_rus_name
        })

        return df

    except Exception as e:
        print(f"[Исключение] {e}")
        return None

# Пример использования
df = parse_from_data_js("Сбербанк")
if df is not None:
    print(df.head())
else:
    print("Не удалось получить данные.")
