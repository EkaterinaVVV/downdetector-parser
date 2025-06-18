import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime

def parse_downdetector_info(service_name, threshold=100):
    url = f"https://downdetector.info/{service_name}"
    headers = {"User-Agent": "Mozilla/5.0"}

    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.text, 'html.parser')

    table = soup.find("table", class_="table")
    if table is None:
        print(f"[Ошибка] Таблица не найдена на странице {url}")
        return None

    data = []
    for row in table.find("tbody").find_all("tr"):
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
            "num_reports": count,
            "incident_occurred": count > threshold,
            "service_name": service_name
        })

    return pd.DataFrame(data)


def main():
    with open("services.txt", "r") as f:
        services = [line.strip() for line in f if line.strip()]

    all_dfs = []
    for service in services:
        df = parse_downdetector_info(service)
        if df is not None:
            all_dfs.append(df)

    if all_dfs:
        result = pd.concat(all_dfs)
        today = datetime.now().strftime("%Y-%m-%d")
        result.to_csv(f"data/downdetector_{today}.csv", index=False)
        print(f"[✓] Данные успешно сохранены: data/downdetector_{today}.csv")
    else:
        print("Нет данных для сохранения.")

if __name__ == "__main__":
    main()
