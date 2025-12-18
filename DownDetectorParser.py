#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Полностью переписанный и исправленный парсер для detector404.ru
Включает:
 - надёжный парсер сообщений пользователей (опирается на span[data-text])
 - корректный разбор относительных и абсолютных времён
 - создание CSV-файлов с заголовками при первом запуске
 - сохранение в SQLite и в CSV
 - формирование Excel и отправка в Telegram (если задан токен)

Запуск: python detector404_parser_fixed.py
Требования: selenium, webdriver-manager, beautifulsoup4, pandas, openpyxl, python-dateutil, pytz
"""

import os
import time
import re
import pytz
import sqlite3
import pandas as pd
import requests
import zipfile
from datetime import datetime
from dateutil.relativedelta import relativedelta
from bs4 import BeautifulSoup
from contextlib import closing

from selenium import webdriver
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from webdriver_manager.firefox import GeckoDriverManager
from selenium.common.exceptions import TimeoutException

from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.chrome.options import Options as ChromeOptions
from webdriver_manager.chrome import ChromeDriverManager


# ===================== НАСТРОЙКИ =====================
SERVICES = [
    "sberbank", "tinkoff", "bank-vtb", "vkontakte", "snapchat", "facebook",
    "mts", "bilajn", "rostelekom", "ozon", "wildberries"
]
DAYS_BACK = 0

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.join(SCRIPT_DIR, "parsed_data")
os.makedirs(BASE_DIR, exist_ok=True)

CSV_FILES = {
    "graph": "graph_data.csv",
    "cloud": "cloud_tags.csv",
    "hist": "histograms.csv",
    "messages": "user_messages.csv",
}
CSV_FILES = {k: os.path.join(BASE_DIR, v) for k, v in CSV_FILES.items()}
DB_PATH = os.path.join(BASE_DIR, "all_parsed_data.db")

MSK = pytz.timezone("Europe/Moscow")
RUN_DATE = datetime.now(MSK).strftime("%Y-%m-%d")
NOW = datetime.now(MSK)
START_DATE = NOW.date() - relativedelta(days=DAYS_BACK)


# ===================== SQLITE HELPERS =====================

def init_sqlite_tables():
    """Инициализация таблиц SQLite один раз при запуске"""
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS graph_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                company TEXT,
                parse_date TEXT,
                parse_time TEXT,
                complaints INTEGER,
                failures REAL
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS cloud_tags (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                company TEXT,
                parse_date TEXT,
                word TEXT,
                frequency REAL
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS histograms (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                company TEXT,
                parse_date TEXT,
                type TEXT,
                name TEXT,
                percent REAL
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS user_messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                company TEXT,
                message_date TEXT,
                message_time TEXT,
                nickname TEXT,
                comment TEXT
            )
        ''')
        conn.commit()


def append_to_sqlite(table_name, data):
    """Добавление данных в SQLite порциями"""
    if not data:
        return
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        if table_name == "graph_data":
            cursor.executemany(
                "INSERT INTO graph_data (company, parse_date, parse_time, complaints, failures) VALUES (?, ?, ?, ?, ?)",
                data
            )
        elif table_name == "cloud_tags":
            cursor.executemany(
                "INSERT INTO cloud_tags (company, parse_date, word, frequency) VALUES (?, ?, ?, ?)",
                data
            )
        elif table_name == "histograms":
            cursor.executemany(
                "INSERT INTO histograms (company, parse_date, type, name, percent) VALUES (?, ?, ?, ?, ?)",
                data
            )
        elif table_name == "user_messages":
            cursor.executemany(
                "INSERT INTO user_messages (company, message_date, message_time, nickname, comment) VALUES (?, ?, ?, ?, ?)",
                data
            )
        conn.commit()


def append_to_csv(csv_path, data, headers):
    """Добавление данных в CSV файл порциями"""
    if not data:
        return
    df = pd.DataFrame(data, columns=headers)
    if os.path.exists(csv_path):
        df.to_csv(csv_path, mode='a', header=False, index=False, sep=';', encoding='utf-8-sig')
    else:
        df.to_csv(csv_path, index=False, sep=';', encoding='utf-8-sig')


# ===================== HELPERS =====================

def ensure_csv_files_exist():
    """Создать CSV-файлы с заголовками, если их ещё нет — полезно при первом запуске."""
    headers_map = {
        "graph": ["Компания", "Дата парсинга", "Время", "Жалобы", "Сбои"],
        "cloud": ["Компания", "Дата парсинга", "Слово", "Частота"],
        "hist":  ["Компания", "Дата парсинга", "Тип", "Название", "Процент"],
        "messages": ["Компания", "Дата парсинга", "Время", "Ник", "Комментарий"],
    }
    for key, path in CSV_FILES.items():
        if not os.path.exists(path):
            df = pd.DataFrame(columns=headers_map.get(key, []))
            df.to_csv(path, index=False, sep=';', encoding='utf-8-sig')


def normalize_percent(value):
    """Нормализация процентных значений"""
    if not value:
        return None
    value = value.replace("%", "").replace(",", ".").strip()
    try:
        return float(value)
    except ValueError:
        return None


def parse_relative_time(text):
    """Парсинг относительного или абсолютного времени.
    Поддерживает:
      - "только что", "25 минут назад", "2 часа назад" и т.п.
      - абсолютные даты "17.12.2025, 03:46"
      - числовые метки в миллисекундах (например 1765931314342)
    Возвращает datetime с таймзоной MSK или None при нераспознавании.
    """
    if not text:
        return None
    text = text.strip()
    # "только что"
    if "только" in text:
        return NOW
    # Числовая метка (миллисекунды или секунды)
    if re.fullmatch(r"\d{10,16}", text):
        try:
            ts = int(text)
            if ts > 10**11:
                dt = datetime.fromtimestamp(ts / 1000, tz=pytz.UTC).astimezone(MSK)
            else:
                dt = datetime.fromtimestamp(ts, tz=pytz.UTC).astimezone(MSK)
            return dt
        except Exception:
            pass
    # Абсолютная дата формата "17.12.2025, 03:46" или "17.12.2025 03:46"
    m = re.search(r"(\d{1,2}\.\d{1,2}\.\d{4})[,\s]+(\d{1,2}:\d{2})", text)
    if m:
        try:
            dt = datetime.strptime(f"{m.group(1)} {m.group(2)}", "%d.%m.%Y %H:%M")
            dt = MSK.localize(dt)
            return dt
        except Exception:
            pass
    # Относительные форматы: "25 минут назад", "2 часа назад" и т.п.
    rel_match = re.match(r"(\d+)\s*(минут|минуты|час|часа|часов|дн|дня|дней|дн\.|недел|неделя|недель|месяц|месяца|месяцев|год|года|лет)", text)
    if rel_match:
        num = int(rel_match.group(1))
        unit = rel_match.group(2)
        try:
            if "мин" in unit:
                return NOW - relativedelta(minutes=num)
            if "час" in unit:
                return NOW - relativedelta(hours=num)
            if unit.startswith("дн") or "дня" in unit or "дней" in unit:
                return NOW - relativedelta(days=num)
            if "нед" in unit:
                return NOW - relativedelta(weeks=num)
            if "месяц" in unit:
                return NOW - relativedelta(months=num)
            if "год" in unit or "лет" in unit:
                return NOW - relativedelta(years=num)
        except Exception:
            return None
    return None


# ===================== PARSING FUNCTIONS =====================

def parse_graph_data(driver, service):
    """Парсинг данных графика"""
    graph_js = """
    const canvas = document.querySelector('canvas[data-err="сетевые сбои"]');
    const chart = Chart.getChart(canvas);
    return chart ? chart.data : null;
    """
    try:
        chart = driver.execute_script(graph_js)
    except Exception:
        chart = None
    if not chart:
        return []
    graph_data = []
    if len(chart["datasets"]) >= 2:
        for c, n in zip(chart["datasets"][0]["data"], chart["datasets"][1]["data"]):
            dt = datetime.fromtimestamp(c["x"] / 1000, tz=pytz.UTC).astimezone(MSK)
            graph_data.append([
                service,
                dt.strftime("%Y-%m-%d"),
                dt.strftime("%H:%M:%S"),
                int(c["y"]),
                float(n["y"])
            ])
    elif len(chart["datasets"]) > 0:
        for c in chart["datasets"][0]["data"]:
            dt = datetime.fromtimestamp(c["x"] / 1000, tz=pytz.UTC).astimezone(MSK)
            graph_data.append([
                service,
                dt.strftime("%Y-%m-%d"),
                dt.strftime("%H:%M:%S"),
                int(c["y"]),
                float(0)
            ])
    return graph_data


def parse_cloud_tags(driver):
    """Парсинг облака тегов"""
    cloud_js = """
    return [...document.querySelectorAll('.bow svg text')]
    .map(t => ({
        word: t.dataset.word,
        freq: parseFloat(t.dataset.freq) * 100
    }));
    """
    try:
        tags = driver.execute_script(cloud_js)
    except Exception:
        tags = None
    return tags or []


def parse_histograms(soup, service):
    """Парсинг гистограмм"""
    hist_data = []
    for span in soup.select("label span.region"):
        percent = normalize_percent(span.get("data-pos", ""))
        if percent is not None:
            hist_data.append([
                service, RUN_DATE, "Регион",
                span.find_previous("a").text.strip(), percent
            ])
    for span in soup.select("label span.cause"):
        percent = normalize_percent(span.get("data-pos", ""))
        if percent is not None:
            hist_data.append([
                service, RUN_DATE, "Неполадка",
                span.find_previous("a").text.strip(), percent
            ])
    for span in soup.select("div.os span[data-size]"):
        text = span.text.strip()
        if "%" in text:
            raw, name = text.split("%", 1)
            percent = normalize_percent(raw)
            if percent is not None:
                hist_data.append([
                    service, RUN_DATE, "Устройство",
                    name.strip(), percent
                ])
    return hist_data


# ===================== PARSER FOR USER MESSAGES (FIXED) =====================

def parse_user_messages(driver, wait, service, max_clicks=100):
    """Надёжный парсер сообщений: сначала парсим видимые сообщения, затем кликаем 'Показать ещё' и парсим добавившиеся id."""
    messages_data = []
    seen_ids = set()
    consecutive_failures = 0
    max_consecutive_failures = 4

    def parse_ids_from_report(report, ids_list, out_messages, seen_set):
        parsed = 0
        for mid in ids_list:
            try:
                if not mid or mid in seen_set:
                    continue
                span_id = report.find("span", attrs={"data-text": mid})
                if not span_id:
                    continue
                author_span = span_id.find_next(lambda tag: tag.name == "span" and tag.has_attr("data-author"))
                nick = (author_span.text.strip() if author_span else "Гость")
                time_span = span_id.find_next(lambda tag: tag.name == "span" and tag.has_attr("data-tick"))
                dt = parse_relative_time(time_span.text.strip() if time_span else "")
                text_div = None
                if time_span:
                    text_div = time_span.find_next(lambda tag: tag.name == "div")
                if not text_div:
                    text_div = span_id.find_next("div")
                if not dt or not text_div:
                    continue
                if dt.date() < START_DATE:
                    return parsed, True
                msg_text = text_div.text.strip()
                out_messages.append([service, dt.strftime("%Y-%m-%d"), dt.strftime("%H:%M:%S"), nick, msg_text])
                seen_set.add(mid)
                parsed += 1
            except Exception as e:
                print(f"Ошибка парсинга сообщения id={mid}: {e}")
                continue
        return parsed, False

    # 1) Парсим начальные видимые сообщения
    soup_init = BeautifulSoup(driver.page_source, "html.parser")
    report_init = soup_init.find("div", class_="report") or soup_init
    ids_init = [s.get("data-text") for s in report_init.find_all("span", attrs={"data-text": True})]
    if ids_init:
        parsed_count, reached_old = parse_ids_from_report(report_init, ids_init, messages_data, seen_ids)
        if parsed_count:
            print(f"Парсинг начального блока: найдено {parsed_count} сообщений, сохраняем.")
            save_messages_batch(messages_data)
            messages_data.clear()
        if reached_old:
            print("В начальном блоке обнаружены сообщения старше START_DATE — завершаем.")
            return

    # 2) Основной цикл кликов
    for attempt in range(max_clicks):
        print(f"\nПопытка загрузки #{attempt + 1}")
        try:
            button = wait.until(
                EC.presence_of_element_located(
                    (By.XPATH, "//button[contains(@data-title, 'Показать') or contains(@aria-label, 'Показать') or contains(text(), 'Показать')]")
                )
            )
        except Exception as e:
            print(f"Кнопка не найдена: {e}")
            break

        soup_before = BeautifulSoup(driver.page_source, "html.parser")
        report_before = soup_before.find("div", class_="report") or soup_before
        ids_before = [s.get("data-text") for s in report_before.find_all("span", attrs={"data-text": True})]
        set_before = set(filter(None, ids_before))

        try:
            last_before = button.get_attribute("data-last")
        except Exception:
            last_before = None

        try:
            driver.execute_script("arguments[0].scrollIntoView({behavior: 'auto', block: 'center'});", button)
            time.sleep(0.15)
            driver.execute_script("arguments[0].click();", button)
            print("Клик выполнен")
        except Exception as e:
            print(f"Не удалось кликнуть по кнопке: {e}")
            consecutive_failures += 1
            if consecutive_failures >= max_consecutive_failures:
                print("Прерывем из-за повторных ошибок клика")
                break
            time.sleep(2)
            continue

        try:
            def progress_detected(drv):
                try:
                    btn = drv.find_element(By.XPATH, "//button[contains(@data-title, 'Показать') or contains(@aria-label, 'Показать') or contains(text(), 'Показать')]")
                    last_now = btn.get_attribute("data-last")
                    if last_before and last_now and last_now != last_before:
                        return True
                except Exception:
                    pass
                try:
                    return len(drv.find_elements(By.CSS_SELECTOR, "span[data-text]")) > len(ids_before)
                except Exception:
                    return False

            wait_long = WebDriverWait(driver, 45, poll_frequency=0.5)
            try:
                wait_long.until(progress_detected)
                print("Обнаружено изменение (data-last или новые span[data-text])")
            except TimeoutException:
                print("За таймаут не произошло заметного прогресса. Обработаем DOM в любом случае.")
        except Exception as e:
            print(f"Ошибка ожидания прогресса: {e}")

        time.sleep(0.6)

        soup_after = BeautifulSoup(driver.page_source, "html.parser")
        report_after = soup_after.find("div", class_="report") or soup_after
        ids_after = [s.get("data-text") for s in report_after.find_all("span", attrs={"data-text": True})]

        new_ids = [i for i in ids_after if i and i not in set_before]
        print(f"Найдено новых id сообщений: {len(new_ids)}")

        try:
            last_after = driver.find_element(By.XPATH, "//button[contains(@data-title, 'Показать') or contains(@aria-label, 'Показать') or contains(text(), 'Показать')]").get_attribute("data-last")
        except Exception:
            last_after = None

        if not new_ids:
            if last_before and last_after and last_before == last_after:
                print("data-last не изменился и новых id нет — достигнут конец или загрузка не продвигается.")
                break
            else:
                consecutive_failures += 1
                print(f"Новых id нет. Попытка {consecutive_failures}/{max_consecutive_failures}")
                if consecutive_failures >= max_consecutive_failures:
                    print("Прерываем из-за отсутствия прогресса")
                    break
                time.sleep(1.2)
                continue

        parsed_count, reached_old = parse_ids_from_report(report_after, new_ids, messages_data, seen_ids)
        print(f"Спарсено {parsed_count} новых сообщений (из {len(new_ids)} найденных id)")
        if parsed_count:
            save_messages_batch(messages_data)
            messages_data.clear()
        if reached_old:
            print("Достигнут блок со старыми сообщениями — завершаем.")
            return

        consecutive_failures = 0
        time.sleep(0.8)

    if messages_data:
        save_messages_batch(messages_data)

    print(f"Завершено. Всего уникальных id: {len(seen_ids)}")


# ===================== Сохранение пачки сообщений =====================

def save_messages_batch(messages_data):
    """Сохранение порции сообщений"""
    if messages_data:
        append_to_csv(CSV_FILES["messages"], messages_data,
                     ["Компания", "Дата парсинга", "Время", "Ник", "Комментарий"])
        append_to_sqlite("user_messages", messages_data)


# ===================== MAIN PARSING LOOP =====================

def main():
    """Основная функция парсинга"""
    init_sqlite_tables()
    ensure_csv_files_exist()

    options = ChromeOptions()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080")

    with webdriver.Chrome(
        service=ChromeService(ChromeDriverManager().install()),
        options=options
    ) as driver:
        wait = WebDriverWait(driver, 60)
        for service in SERVICES:
            print(f"\n=== Парсинг {service.upper()} ===")
            try:
                driver.get(f"https://detector404.ru/{service}")
                time.sleep(4)

                graph_data = parse_graph_data(driver, service)
                if graph_data:
                    append_to_csv(CSV_FILES["graph"], graph_data,
                                ["Компания", "Дата парсинга", "Время", "Жалобы", "Сбои"])
                    append_to_sqlite("graph_data", graph_data)

                tags = parse_cloud_tags(driver)
                if tags:
                    cloud_data = [[service, RUN_DATE, t.get("word"), round(float(t.get("freq", 0)), 2)] for t in tags]
                    append_to_csv(CSV_FILES["cloud"], cloud_data,
                                ["Компания", "Дата парсинга", "Слово", "Частота"])
                    append_to_sqlite("cloud_tags", cloud_data)

                soup = BeautifulSoup(driver.page_source, "html.parser")
                hist_data = parse_histograms(soup, service)
                if hist_data:
                    append_to_csv(CSV_FILES["hist"], hist_data,
                                ["Компания", "Дата парсинга", "Тип", "Название", "Процент"])
                    append_to_sqlite("histograms", hist_data)

                parse_user_messages(driver, wait, service)

            except Exception as e:
                print(f"Ошибка при парсинге {service}: {e}")
                continue

    create_excel_report()
    send_to_telegram()


# ===================== Создание Excel файла =====================

def create_excel_report():
    """Создание итогового Excel файла"""
    excel_path = os.path.join(BASE_DIR, "all_parsed_data.xlsx")
    sheets_data = {}
    for sheet_name, csv_path in CSV_FILES.items():
        if os.path.exists(csv_path) and os.path.getsize(csv_path) > 0:
            try:
                df = pd.read_csv(csv_path, sep=";", encoding="utf-8-sig")
                if not df.empty:
                    sheets_data[sheet_name] = df
            except Exception as e:
                print(f"Ошибка чтения {csv_path}: {e}")
    if sheets_data:
        with pd.ExcelWriter(excel_path, engine="openpyxl") as writer:
            for sheet_name, df in sheets_data.items():
                df.to_excel(writer, sheet_name=sheet_name, index=False)
        print(f"\nExcel-файл создан: {excel_path}")
        return excel_path
    return None


# ===================== Отправка в Telegram =====================

def send_to_telegram(excel_path=None):
    """Отправка результатов в Telegram"""
    TELEGRAM_TOKEN = "8504851639:AAEVAQ0JpfgMbp-_ah0sHuSJ0WJmDEi0Ql4"
    CHAT_ID = 1079939449
    def send_message(text):
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        try:
            requests.post(url, json={"chat_id": CHAT_ID, "text": text}, timeout=10)
        except Exception as e:
            print(f"Ошибка отправки сообщения: {e}")
    def send_file(file_path):
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendDocument"
        try:
            with open(file_path, "rb") as f:
                requests.post(url, data={"chat_id": CHAT_ID}, files={"document": f}, timeout=30)
        except Exception as e:
            print(f"Ошибка отправки файла: {e}")
    ZIP_NAME = f"detector404_parsing_{RUN_DATE}.zip"
    ZIP_PATH = os.path.join(BASE_DIR, ZIP_NAME)
    try:
        with zipfile.ZipFile(ZIP_PATH, "w", zipfile.ZIP_DEFLATED) as zipf:
            for file_name in os.listdir(BASE_DIR):
                full_path = os.path.join(BASE_DIR, file_name)
                if (os.path.isfile(full_path) and file_name != ZIP_NAME and not file_name.endswith(".zip")):
                    zipf.write(full_path, arcname=file_name)
        send_message(f"Парсинг detector404 завершён.\nДата: {RUN_DATE}\nСервисов: {len(SERVICES)}")
        if excel_path:
            send_file(excel_path)
        else:
            send_file(ZIP_PATH)
        send_message("Финальный ZIP-архив успешно отправлен.")
        if os.path.exists(ZIP_PATH):
            os.remove(ZIP_PATH)
    except Exception as e:
        print(f"Ошибка создания/отправки архива: {e}")
        send_message(f"Ошибка при отправке результатов: {e}")


if __name__ == "__main__":
    main()
