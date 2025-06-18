import requests
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
import re

def parse_downdetector_service(service_rus_name: str) -> pd.DataFrame | None:
    # Пример: 'Сбербанк' → https://downdetector.info/data.js?service=Сбербанк
    url = f"https://downdetector.info/data.js?service={service_rus_name}"

    headers = {
        "User-Agent": "Mozilla/5.0"
    }

    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        print(f"[Ошибка] Не удалось получить данные: {url}")
        return None

    text = response.text

    try:
        # Ищем строки с JS-массивами
        hours_match = re.search(r"var hourlabels = (\[.*?\]);", text)
        values_match = re.search(r"var datavalues = (\[.*?\]);", text)

        if not hours_match or not values_match:
            print("[Ошибка] Не удалось найти данные внутри скрипта")
            return None

        hours = eval(hours_match.group(1))
        values = eval(values_match.group(1))

        now = datetime.now().strftime("%Y-%m-%d")

        df = pd.DataFrame({
            "datetime": [f"{now} {h}:00" for h in hours],
            "num_reports": values,
            "service": service_rus_name,
            "scrape_time": now
        })

        return df

    except Exception as e:
        print(f"[Ошибка парсинга]: {e}")
        return None

# Пример использования
df = parse_downdetector_service("Сбербанк")
if df is not None:
    print(df.head())
    df.to_csv("sberbank_reports.csv", index=False)
