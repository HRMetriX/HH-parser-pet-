import requests
import pandas as pd
import time
from datetime import datetime, timedelta, timezone
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

headers = {"User-Agent": "MemeWeather-HH-Pipeline/1.0 (oborisov.personal@gmail.com)"}

def load_regions_and_cities_from_api():
    url = "https://api.hh.ru/areas/113"  # Исправлено: убраны пробелы
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        print(f"Ошибка при загрузке регионов: {response.status_code}")
        return []
    data = response.json()
    regions = []
    def extract_regions(areas):
        for area in areas:
            if area.get("areas"):
                regions.append(area["id"])
                extract_regions(area["areas"])
    extract_regions(data["areas"])
    return regions

# === Функция сбора ===
def get_vacancies_for_date(area_id, date_str, search_text="аналитик"):
    url = "https://api.hh.ru/vacancies"
    params = {
        "text": search_text,
        "area": area_id,
        "search_field": "name",
        "date_from": date_str,
        "date_to": date_str,
        "per_page": 100,
        "page": 0
    }

    # Настройка повторных попыток
    session = requests.Session()
    retries = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    session.mount("https://", HTTPAdapter(max_retries=retries))

    all_vac = []
    page = 0
    while page < 5:  # уменьшил до 5 страниц (500 вакансий — более чем достаточно за день)
        params["page"] = page
        try:
            resp = session.get(url, headers=headers, params=params, timeout=10)
            if resp.status_code != 200:
                print(f"⚠️ Ошибка {resp.status_code} для area_id={area_id}, page={page}")
                break
            data = resp.json()
            if not data.get("items"):
                break
            all_vac.extend(data["items"])
            page += 1
            time.sleep(0.5)
        except Exception as e:
            print(f"❌ Ошибка при запросе: {e}. Пропускаем area_id={area_id}")
            break  # не зацикливаемся — переходим к следующему региону

    return all_vac

# === Преобразование и загрузка ===
def flatten_vacancy(v):
    flat = {}
    flat["id"] = int(v["id"])
    flat["name"] = v.get("name")
    flat["published_at"] = v.get("published_at")
    flat["created_at"] = v.get("created_at")
    flat["url"] = v.get("alternate_url")
    flat["archived"] = v.get("archived", False)
    flat["area_name"] = v.get("area", {}).get("name")
    salary = v.get("salary") or {}
    flat["salary_from"] = salary.get("from")
    flat["salary_to"] = salary.get("to")
    flat["salary_currency"] = salary.get("currency")
    flat["salary_gross"] = salary.get("gross")
    flat["employer_name"] = v.get("employer", {}).get("name")
    flat["experience_name"] = v.get("experience", {}).get("name")
    flat["employment_name"] = v.get("employment", {}).get("name")
    flat["schedule_name"] = v.get("schedule", {}).get("name")
    roles = v.get("professional_roles") or []
    flat["role_name"] = roles[0].get("name") if roles else None
    # Добавим area_id, employer_id, role_id, чтобы их можно было удалить позже, как в backfill
    flat["area_id"] = v.get("area", {}).get("id")
    flat["employer_id"] = v.get("employer", {}).get("id")
    flat["role_id"] = roles[0].get("id") if roles else None
    return flat
