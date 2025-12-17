import os
import time
import re
import pytz
import sqlite3
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
from bs4 import BeautifulSoup
import requests
from selenium import webdriver
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from webdriver_manager.firefox import GeckoDriverManager


# ===================== НАСТРОЙКИ =====================

SERVICES = ["mts", "tele2", "telegram", "roblox", "whatsapp"]
DAYS_BACK = 0

BASE_DIR = "parsed_data"
os.makedirs(BASE_DIR, exist_ok=True)

CSV_FILES = {
    "graph": "graph_data.csv",
    "cloud": "cloud_tags.csv",
    "hist": "histograms.csv",
    "messages": "user_messages.csv",
}

CSV_FILES = {k: os.path.join(BASE_DIR, v) for k, v in CSV_FILES.items()}
DB_PATH = os.path.join(BASE_DIR, "all_parsed_data.db")

RUN_DATE = datetime.now().strftime("%Y-%m-%d")
NOW = datetime.now()
MSK = pytz.timezone("Europe/Moscow")
START_DATE = NOW.date() - relativedelta(days=DAYS_BACK)


# ===================== BROWSER =====================

options = Options()
options.add_argument("--headless")
options.add_argument("--window-size=1920,1080")

driver = webdriver.Firefox(
    service=Service(GeckoDriverManager().install()),
    options=options
)

wait = WebDriverWait(driver, 30)

# ===================== HELPERS =====================

def append_csv_and_sqlite(df, csv_path, table_name):
    # CSV
    if os.path.exists(csv_path):
        df.to_csv(csv_path, mode="a", header=False, index=False, sep=";", encoding="utf-8-sig")
    else:
        df.to_csv(csv_path, index=False, sep=";", encoding="utf-8-sig")

    # SQLite
    with sqlite3.connect(DB_PATH) as conn:
        df.to_sql(table_name, conn, if_exists="append", index=False)


def normalize_percent(value):
    if not value:
        return None
    value = value.replace("%", "").replace(",", ".").strip()
    try:
        return float(value)
    except ValueError:
        return None


def parse_relative_time(text):
    if "только что" in text:
        return NOW

    match = re.match(r"(\d+)\s*(минут|час|дн|недел|месяц|год)", text)
    if not match:
        return None

    num = int(match.group(1))
    unit = match.group(2)

    if "минут" in unit:
        return NOW - relativedelta(minutes=num)
    if "час" in unit:
        return NOW - relativedelta(hours=num)
    if "дн" in unit:
        return NOW - relativedelta(days=num)
    if "недел" in unit:
        return NOW - relativedelta(weeks=num)
    if "месяц" in unit:
        return NOW - relativedelta(months=num)
    if "год" in unit:
        return NOW - relativedelta(years=num)

    return None


# ===================== MAIN =====================

try:
    for service in SERVICES:
        print(f"\n=== Парсинг {service.upper()} ===")
        driver.get(f"https://detector404.ru/{service}")
        time.sleep(5)

        soup = BeautifulSoup(driver.page_source, "html.parser")

        # ---------- GRAPH ----------
        graph_js = """
        const canvas = document.querySelector('canvas[data-err="сетевые сбои"]');
        const chart = Chart.getChart(canvas);
        return chart ? chart.data : null;
        """
        chart = driver.execute_script(graph_js)

        if chart and len(chart["datasets"]) >= 2:
            rows = []
            for c, n in zip(chart["datasets"][0]["data"], chart["datasets"][1]["data"]):
                dt = datetime.fromtimestamp(c["x"] / 1000, tz=pytz.UTC).astimezone(MSK)
                rows.append([
                    service,
                    dt.strftime("%Y-%m-%d"),
                    dt.strftime("%H:%M:%S"),
                    int(c["y"]),
                    float(n["y"])
                ])

            df = pd.DataFrame(rows, columns=[
                "Компания", "Дата парсинга", "Время", "Жалобы", "Сбои"
            ])
            append_csv_and_sqlite(df, CSV_FILES["graph"], "graph_data")
        elif chart and len(chart["datasets"]) > 0:
            rows = []
            for c in chart["datasets"][0]["data"]:
                dt = datetime.fromtimestamp(c["x"] / 1000, tz=pytz.UTC).astimezone(MSK)
                rows.append([
                    service,
                    dt.strftime("%Y-%m-%d"),
                    dt.strftime("%H:%M:%S"),
                    int(c["y"]),
                    float(0)
                ])

            df = pd.DataFrame(rows, columns=[
                "Компания", "Дата парсинга", "Время", "Жалобы", "Сбои"
            ])
            append_csv_and_sqlite(df, CSV_FILES["graph"], "graph_data")

        # ---------- CLOUD TAGS ----------
        cloud_js = """
        return [...document.querySelectorAll('.bow svg text')]
        .map(t => ({
            word: t.dataset.word,
            freq: parseFloat(t.dataset.freq) * 100
        }));
        """
        tags = driver.execute_script(cloud_js)

        if tags:
            df = pd.DataFrame(
                [[service, RUN_DATE, t["word"], round(float(t["freq"]), 2)] for t in tags],
                columns=["Компания", "Дата парсинга", "Слово", "Частота"]
            )
            append_csv_and_sqlite(df, CSV_FILES["cloud"], "cloud_tags")

        # ---------- HISTOGRAMS ----------
        rows = []

        for span in soup.select("label span.region"):
            percent = normalize_percent(span.get("data-pos", ""))
            if percent is not None:
                rows.append([service, RUN_DATE, "Регион",
                             span.find_previous("a").text.strip(), percent])

        for span in soup.select("label span.cause"):
            percent = normalize_percent(span.get("data-pos", ""))
            if percent is not None:
                rows.append([service, RUN_DATE, "Неполадка",
                             span.find_previous("a").text.strip(), percent])

        for span in soup.select("div.os span[data-size]"):
            text = span.text.strip()
            if "%" in text:
                raw, name = text.split("%", 1)
                percent = normalize_percent(raw)
                if percent is not None:
                    rows.append([service, RUN_DATE, "Устройство", name.strip(), percent])

        if rows:
            df = pd.DataFrame(rows, columns=[
                "Компания", "Дата парсинга", "Тип", "Название", "Процент"
            ])
            append_csv_and_sqlite(df, CSV_FILES["hist"], "histograms")

        # ---------- USER MESSAGES ----------

        last_html = ""
        stop_loading = False

        while True:
            current_html = driver.page_source

            if current_html == last_html:
                break

            last_html = current_html
            soup = BeautifulSoup(current_html, "html.parser")

            spans = soup.select("div.report span")
            for i in range(len(spans)):
                if spans[i].has_attr("data-author"):
                    time_span = spans[i + 1]
                    dt = parse_relative_time(time_span.text.strip())
                    if dt and dt.date() < START_DATE:
                        stop_loading = True
                        break

            if stop_loading:
                break

            try:
                button = wait.until(EC.presence_of_element_located(
                    (By.XPATH, "//button[@data-title='Показать ещё']")
                ))
                driver.execute_script("arguments[0].scrollIntoView(true);", button)
                time.sleep(5)
                driver.execute_script("arguments[0].click();", button)
                time.sleep(10)

            except:
                break

        soup = BeautifulSoup(driver.page_source, "html.parser")
        report = soup.find("div", class_="report")

        if report:
            rows = []
            spans = list(report.find_all("span"))

            for i in range(len(spans)):
                if spans[i].has_attr("data-author"):
                    nick = spans[i].text.strip() or "Гость"
                    time_span = spans[i + 1]
                    dt = parse_relative_time(time_span.text.strip())
                    text_div = time_span.find_next("div")

                    if dt and dt.date() >= START_DATE:
                        rows.append([
                            service,
                            dt.strftime("%Y-%m-%d"),
                            dt.strftime("%H:%M:%S"),
                            nick,
                            text_div.text.strip()
                        ])

            if rows:
                df = pd.DataFrame(rows, columns=[
                    "Компания", "Дата парсинга", "Время", "Ник", "Комментарий"
                ])
                append_csv_and_sqlite(df, CSV_FILES["messages"], "user_messages")

finally:
    driver.quit()

# ===================== EXCEL AGGREGATION =====================

excel_path = os.path.join(BASE_DIR, "all_parsed_data.xlsx")

def safe_read_csv(path):
    if os.path.exists(path):
        return pd.read_csv(path, sep=";", encoding="utf-8-sig")
    return pd.DataFrame()

df_graph = safe_read_csv(CSV_FILES["graph"])
df_cloud = safe_read_csv(CSV_FILES["cloud"])
df_hist = safe_read_csv(CSV_FILES["hist"])
df_messages = safe_read_csv(CSV_FILES["messages"])

with pd.ExcelWriter(excel_path, engine="openpyxl") as writer:
    if not df_graph.empty:
        df_graph.to_excel(writer, sheet_name="graph_data", index=False)
    if not df_cloud.empty:
        df_cloud.to_excel(writer, sheet_name="cloud_tags", index=False)
    if not df_hist.empty:
        df_hist.to_excel(writer, sheet_name="histograms", index=False)
    if not df_messages.empty:
        df_messages.to_excel(writer, sheet_name="user_messages", index=False)


print(f"\nExcel-файл обновлён: {excel_path}")

TG_BOT_TOKEN = os.getenv("TELEGRAM_TOKEN")
TG_CHAT_ID = int(os.getenv("TG_CHAT_ID"))

if not TG_BOT_TOKEN or not TG_CHAT_ID:
    raise RuntimeError("Telegram credentials not set")

def tg_send_message(text: str):
    if not TG_BOT_TOKEN or not TG_CHAT_ID:
        print("TG_BOT_TOKEN / TG_CHAT_ID не заданы, пропускаю отправку в Telegram.")
        return

    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
    resp = requests.post(url, data={"chat_id": TG_CHAT_ID, "text": text}, timeout=60)
    resp.raise_for_status()

def tg_send_file(file_path: str, caption: str = ""):
    if not TG_BOT_TOKEN or not TG_CHAT_ID:
        print("TG_BOT_TOKEN / TG_CHAT_ID не заданы, пропускаю отправку файла в Telegram.")
        return

    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Файл не найден: {file_path}")

    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendDocument"
    with open(file_path, "rb") as f:
        files = {"document": (os.path.basename(file_path), f)}
        data = {"chat_id": TG_CHAT_ID, "caption": caption}
        resp = requests.post(url, data=data, files=files, timeout=300)
    resp.raise_for_status()

# ---- ВЫЗОВ ПОСЛЕ СОЗДАНИЯ EXCEL ----
try:
    caption = f"✅ DownDetector: файл обновлён {RUN_DATE}"
    tg_send_file(excel_path, caption=caption)
    tg_send_message("✅ Парсинг завершён без ошибок, файл отправила.")
except Exception as e:
    # если упало на отправке — хотя бы сообщим
    try:
        tg_send_message(f"❌ Ошибка при отправке файла: {e}")
    except Exception:
        pass
    raise





