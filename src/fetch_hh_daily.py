import requests
import time
from datetime import datetime, timedelta
from supabase import create_client
import os
import pytz # pip install pytz
import pandas as pd # pip install pandas
import sys
# Добавляем путь
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from utils.utils_daily import load_regions_and_cities_from_api, flatten_vacancy, get_vacancies_for_date

# === Настройки ===
headers = {"User-Agent": "MemeWeather-HH-Pipeline/1.0 (oborisov.personal@gmail.com)"}

# Инициализация клиента Supabase (предполагается, что переменные окружения заданы)
supabase = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_KEY"])

# Города-миллионники + регионы (как в backfill)
cMillioners = [1, 2, 1057, 3, 1130, 54, 1679, 139, 1321, 1438, 1586, 1420, 1249, 1844, 1317, 1511]

# Получаем список регионов (как в backfill) - убраны лишние пробелы
''' def load_regions_and_cities_from_api():
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
    return regions '''

all_regions = load_regions_and_cities_from_api()
cMillioners_extended = list(set(cMillioners + all_regions))

# === Вчерашняя дата (с учётом московского времени) ===
moscow_tz = pytz.timezone('Europe/Moscow')
yesterday = (datetime.now(moscow_tz) - timedelta(days=1)).strftime("%Y-%m-%d")
print(f"Собираем вакансии за: {yesterday}")

# === Функция сбора ===
''' def get_vacancies_for_date(area_id, date_str, search_text="аналитик"): # Добавлен search_text
    url = "https://api.hh.ru/vacancies"  # Исправлено: убраны пробелы
    params = {
        "text": search_text,
        "area": area_id,
        "search_field": "name",
        "date_from": date_str,
        "date_to": date_str,
        "per_page": 100,
        "page": 0
    }
    all_vac = []
    page = 0
    while page < 20:
        params["page"] = page
        # print(f"    Запрос страницы {page} для area_id={area_id}, date={date_str}, text={search_text}...") # Убрано
        resp = requests.get(url, headers=headers, params=params)
        if resp.status_code != 200:
            print(f"    Ошибка при запросе к area_id={area_id}, date={date_str}, text={search_text}, page={page}: {resp.status_code}") # Оставлено для отладки ошибок API
            break
        data = resp.json()
        if not data["items"]:
            # print(f"    Нет вакансий на странице {page} для area_id={area_id}, date={date_str}, text={search_text}.") # Убрано
            break
        all_vac.extend(data["items"])
        # print(f"    Получено {len(data['items'])} вакансий на странице {page} для area_id={area_id}, date={date_str}, text={search_text}. Всего накоплено: {len(all_vac)}") # Убрано
        page += 1
        time.sleep(0.3)
    
    # Проверка на лимит (опционально)
    if len(all_vac) == 2000:
        print(f"⚠️  Возможен лимит 2000 вакансий для area_id={area_id}, date={date_str}, text={search_text}")
        
    return all_vac '''

# === Основной сбор ===
all_vacancies = []
seen_ids = set()

search_texts = ["аналитик", "analytic"] # Добавлен "analytic"

for search_text in search_texts:
    # print(f"--- Сбор для текста: {search_text} ---") # Убрано
    # Добавим счётчик для отладки
    area_counter = 0
    total_areas = len(cMillioners_extended)
    for area_id in cMillioners_extended:
        area_counter += 1
        # print(f"  Обработка area_id {area_id} ({area_counter}/{total_areas}) для '{search_text}'...") # Убрано
        batch = get_vacancies_for_date(area_id, yesterday, search_text)
        for v in batch:
            if v["id"] not in seen_ids:
                all_vacancies.append(v)
                seen_ids.add(v["id"])
        # (Опционально) Вывести количество вакансий, собранных для этого area_id
        # print(f"    -> Собрано уникальных вакансий для area_id {area_id}: {len([v for v in batch if v['id'] not in seen_ids])} (новых: до этого было {len(seen_ids) - len([v for v in batch if v['id'] not in seen_ids])})") # Убрано

    # Шаг 2: Финальная проверка по всей России (area=113)
    # print(f"  Финальная проверка area=113 для '{search_text}'...") # Убрано
    batch = get_vacancies_for_date(113, yesterday, search_text)
    initial_seen_count = len(seen_ids)
    for v in batch:
        if v["id"] not in seen_ids:
            all_vacancies.append(v)
            seen_ids.add(v["id"])
    final_seen_count = len(seen_ids)
    # print(f"    -> Собрано уникальных вакансий для area=113: {len(batch)} (новых: {final_seen_count - initial_seen_count})") # Убрано

print(f"✅ Найдено {len(all_vacancies)} вакансий. Обработка...")

# === Преобразование и загрузка ===
''' def flatten_vacancy(v):
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
    return flat '''

records = [flatten_vacancy(v) for v in all_vacancies]

# --- НОВОЕ: Обработка records, аналогичная FinalDF из backfill ---
if records:
    # Создаём DataFrame из records
    df_temp = pd.DataFrame(records)
    
    # Применяем преобразования, аналогичные FinalDF
    FinalRecords = (df_temp
        .astype({
            "salary_from": "Int64",  # Используем nullable integer
            "salary_to": "Int64",
            "id": "int64", # Убедимся, что id int64
            # area_id и employer_id могут быть NaN, поэтому используем Int64 если нужно, или оставим как есть
            # "area_id": "Int64", # Опционально, если нужно привести к Int64
            # "employer_id": "Int64" # Опционально
        })
        .drop(columns=['area_id', 'employer_id', 'role_id'], errors='ignore') # Удаляем колонки, если они есть
        .drop_duplicates() # Убираем дубликаты
        .assign(salary_gross=lambda x: x["salary_gross"].map({
            True: True,
            False: False,
            "true": True,
            "false": False,
            None: None,
            "True": True, # На всякий случай, если API вернёт строку
            "False": False # На всякий случай
        }).astype("boolean")) # Приводим к типу boolean
    )
    
    # Конвертируем обратно в список словарей для upsert
    records = FinalRecords.to_dict('records')
    
    # print(f"✅ Обработано: {len(records)} уникальных вакансий после очистки (было {len(all_vacancies)} до очистки).") # Убрано
else:
    # print("📭 Нет новых вакансий за вчера для обработки.") # Убрано
    pass # Просто не делаем ничего, если records пустой


if records:
    try:
        supabase.table("hh_vacancies").upsert(records, on_conflict="id").execute()
        print(f"✅ Загружено {len(records)} вакансий в Supabase")
    except Exception as e:
        print(f"❌ Ошибка при загрузке в Supabase: {e}")
else:
    print("📭 Нет новых вакансий за вчера для загрузки.")
