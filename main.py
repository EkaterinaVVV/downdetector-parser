from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
import pandas as pd
import time
from datetime import datetime

def parse_downdetector_selenium(service_name: str) -> pd.DataFrame:
    url = f"https://downdetector.info/{service_name}"

    options = Options()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")

    service = Service("chromedriver")  # путь к chromedriver
    driver = webdriver.Chrome(service=service, options=options)

    try:
        driver.get(url)
        time.sleep(5)  # дождаться загрузки

        table = driver.find_element(By.XPATH, "//table")
        rows = table.find_elements(By.TAG_NAME, "tr")

        data = []
        for row in rows[1:]:
            cols = row.find_elements(By.TAG_NAME, "td")
            if len(cols) >= 2:
                hour = cols[0].text.strip()
                count = cols[1].text.strip()
                data.append((hour, int(count)))

        df = pd.DataFrame(data, columns=["time", "num_reports"])
        df["datetime"] = pd.to_datetime(datetime.now().date().strftime("%Y-%m-%d") + " " + df["time"])
        df["service_name"] = service_name
        df["incident_occurred"] = df["num_reports"] > 100
        df["scrape_time"] = datetime.now()

        return df

    except Exception as e:
        print(f"[Ошибка] Не удалось обработать {service_name}: {e}")
        return pd.DataFrame()

    finally:
        driver.quit()
services = ["sberbank"]
all_data = []

for s in services:
    df = parse_downdetector_selenium(s)
    if not df.empty:
        all_data.append(df)

if all_data:
    final_df = pd.concat(all_data)
    os.makedirs("parsed_data", exist_ok=True)
    final_df.to_csv(f"parsed_data/downdetector_{datetime.now().date()}.csv", index=False)
    print("✅ Данные успешно сохранены.")
else:
    print("❌ Нет данных для сохранения.")
