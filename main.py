from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
import pandas as pd
import time
from datetime import datetime

def parse_downdetector(service_name, threshold=100):
    url = f"https://downdetector.info/{service_name}"

    # Настройки Chrome
    options = Options()
    options.add_argument("--headless")  # Без окна браузера
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")

    # Путь до chromedriver (если он лежит рядом — не указывай)
    service = Service(executable_path="./chromedriver")

    # Запуск браузера
    driver = webdriver.Chrome(service=service, options=options)
    driver.get(url)
    time.sleep(5)

    soup = BeautifulSoup(driver.page_source, 'html.parser')
    driver.quit()

    # Парсим таблицу
    table = soup.find("table", class_="table table-bordered table-hover table-striped")
    if not table:
        print(f"[Ошибка] Таблица не найдена: {url}")
        return None

    rows = table.find("tbody").find_all("tr")
    data = []

    for row in rows:
        cols = row.find_all("td")
        if len(cols) < 2:
            continue
        try:
            dt = datetime.strptime(cols[0].text.strip(), "%d.%m.%Y %H:%M")
            count = int(cols[1].text.strip())
        except:
            continue

        data.append({
            "datetime": dt,
            "service_name": service_name,
            "num_reports": count,
            "incident_occurred": count > threshold,
            "scrape_date": datetime.today().strftime("%Y-%m-%d")
        })

    return pd.DataFrame(data)

df = parse_downdetector("sberbank")
df.head()
