import os
import time
import pytz
import pandas as pd
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# Список сервисов
services = {
    "sberbank": "Сбербанк",
    "telegram": "Телеграм",
    "whatsapp": "Ватсапп",
    "vkontakte": "ВКонтакте",
    "tiktok": "ТикТок",
    "tbank": "Т-банк",
    "bank-vtb": "ВТБ Банк",
    "ozon": "Ozon",
    "wildberries": "Wildberries",
    "mts": "МТС",
    "bilajn": "Билайн",
    "megafon": "Мегафон"
}

from selenium.webdriver.chrome.service import Service

def setup_driver():
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/127.0.0.0 Safari/537.36")
    options.binary_location = "/usr/bin/chromium"

    service = Service("/usr/bin/chromedriver")
    return webdriver.Chrome(service=service, options=options)


def parse_service_data(driver, slug, name):
    url = f"https://downdetector.info/{slug}"
    print(f"🔄 Парсинг {name}...")
    driver.get(url)
    WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, "canvas")))
    time.sleep(2)

    chart_data = driver.execute_script("""
        const canvas = document.querySelector('canvas');
        if (canvas) {
            const chart = Chart.getChart(canvas);
            if (chart) {
                return {
                    datasets: chart.data.datasets.map(ds => ({ label: ds.label, data: ds.data }))
                };
            }
        }
        return null;
    """)

    europe_moscow = pytz.timezone("Europe/Moscow")
    data = []
    if chart_data:
        for point in chart_data["datasets"][0]["data"]:
            timestamp_ms = point["x"]
            dt_utc = datetime.fromtimestamp(timestamp_ms / 1000, tz=pytz.UTC)
            dt_local = dt_utc.astimezone(europe_moscow)
            data.append({
                "Дата": dt_local.strftime("%Y-%m-%d %H:%M:%S"),
                "Сервис": name,
                "Жалобы в час": point["y"]
            })
    return data

def main():
    output_path = "all_services_complaints.csv"
    driver = setup_driver()
    all_data = []

    for slug, name in services.items():
        try:
            result = parse_service_data(driver, slug, name)
            all_data.extend(result)
        except Exception as e:
            print(f"❌ Ошибка при парсинге {name}: {e}")

    driver.quit()

    df_new = pd.DataFrame(all_data)

    # if os.path.exists(output_path):
    #     df_old = pd.read_csv(output_path)
    #     df_combined = pd.concat([df_old, df_new], ignore_index=True)
    # else:
    #     df_combined = df_new
    # df_combined.to_csv(output_path, mode='a', index=False, header=not os.path.exists(output_path))
    # # df_combined.to_csv(output_path, index=False)
    # Проверим, существует ли файл
    file_exists = os.path.exists(output_path)

    # Сохраняем новые данные построчно, без объединения с прошлым содержимым
    df_new.to_csv(output_path, mode='a', index=False, header=not file_exists)


    print(f"✅ Данные сохранены в {output_path}")

if __name__ == "__main__":
    main()
