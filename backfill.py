import requests
import pandas as pd
import time
from datetime import datetime, timedelta
from supabase import create_client
import os
import supabase

from utils_backfill import flatten_vacancy, extract_regions_and_cities_recursive, load_regions_and_cities_from_api, get_all_vacancies_for_params

# Заголовок — обязательно с email!
headers = {
    "User-Agent": "MemeWeather-HH-Pipeline/1.0 (oborisov.personal@gmail.com)"
}

''' def flatten_vacancy(v):
    flat = {}

    # Основные поля
    flat["id"] = v.get("id")
    flat["name"] = v.get("name")
    flat["published_at"] = v.get("published_at")
    flat["created_at"] = v.get("created_at")
    flat["url"] = v.get("alternate_url")
    flat["archived"] = v.get("archived", False)

    # Город
    area = v.get("area") or {}
    flat["area_id"] = area.get("id")
    flat["area_name"] = area.get("name")

    # Зарплата
    salary = v.get("salary") or {}
    flat["salary_from"] = salary.get("from")
    flat["salary_to"] = salary.get("to")
    flat["salary_currency"] = salary.get("currency")
    flat["salary_gross"] = salary.get("gross")

    # Работодатель
    employer = v.get("employer") or {}
    flat["employer_name"] = employer.get("name")
    flat["employer_id"] = employer.get("id")

    # Опыт, занятость, график
    exp = v.get("experience") or {}
    emp = v.get("employment") or {}
    sched = v.get("schedule") or {}
    flat["experience_name"] = exp.get("name")
    flat["employment_name"] = emp.get("name")
    flat["schedule_name"] = sched.get("name")

    # Профессиональная роль (берём первую)
    roles = v.get("professional_roles") or []
    flat["role_name"] = roles[0].get("name") if roles else None
    flat["role_id"] = roles[0].get("id") if roles else None

    return flat 

def extract_regions_and_cities_recursive(areas, regions_list, cities_list):
    """
    Рекурсивно извлекает ID регионов (у которых есть 'areas') и конечных городов (у которых 'areas' пустой).
    """
    for area in areas:
        if area.get("areas"):  # если есть подобласти — это регион
            regions_list.append(area["id"])
            # Рекурсивно идём в подобласти
            extract_regions_and_cities_recursive(area["areas"], regions_list, cities_list)
        else:  # если нет подобластей — это конечный город
            cities_list.append(area["id"])

def load_regions_and_cities_from_api():
    url = "https://api.hh.ru/areas/113"
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        print(f"Ошибка при загрузке регионов и городов: {response.status_code}")
        return [], []
    data = response.json()
    regions_list = []
    cities_list = []
    extract_regions_and_cities_recursive(data["areas"], regions_list, cities_list)
    return regions_list, cities_list

def get_all_vacancies_for_params(city_id, search_text, date_from=None, date_to=None):
    url = "https://api.hh.ru/vacancies"
    params = {
        "text": search_text,
        "area": city_id,
        "per_page": 100,
        "page": 0,
        "search_field": "name"
    }
    if date_from:
        params["date_from"] = date_from
    if date_to:
        params["date_to"] = date_to

    all_vacancies = []
    page = 0
    max_pages = 20  # hh.ru возвращает максимум 20 страниц (2000 вакансий)

    while page < max_pages:
        params["page"] = page
        response = requests.get(url, headers=headers, params=params)
        
        if response.status_code != 200:
            print(f"Ошибка на странице {page} при area={city_id}, text='{search_text}', date_from={date_from}, date_to={date_to}: {response.status_code}")
            break
            
        data = response.json()
        vacancies = data["items"]
        if not vacancies:
            break
            
        all_vacancies.extend(vacancies)
        print(f"Страница {page} (area={city_id}, text='{search_text}', date_from={date_from}, date_to={date_to}): получено {len(vacancies)} вакансий (всего: {len(all_vacancies)})")
        
        page += 1
        time.sleep(0.5)  # уважаем API

    print(f"✅ Всего собрано вакансий по area={city_id}, text='{search_text}', date_from={date_from}, date_to={date_to}: {len(all_vacancies)}")
    return all_vacancies '''

# === Основной код ===
print("🔍 Загружаем список всех регионов и городов из API...")
all_regions, all_cities = load_regions_and_cities_from_api()

print(f"Загружено {len(all_regions)} регионов, {len(all_cities)} конечных городов.")

# Подготовленный список городов-миллионников (из твоего старого cMillioners)
cMillioners = [1, 2, 1057, 3, 1130, 54, 1679, 139, 1321, 1438, 1586, 1420, 1249, 1844, 1317, 1511]

# Создаём расширенный список: города-миллионники + все регионы
cMillioners_extended = cMillioners + all_regions

print(f"Используем расширенный список area_id: {len(cMillioners)} городов + {len(all_regions)} регионов = {len(cMillioners_extended)} сущностей.")

all_vacancies = []
seen_ids = set()
# --- НОВОЕ: Для отслеживания "горячих" регионов ---
hot_areas = set()

search_texts = ["аналитик", "analytic"]

# --- Шаг 1: Основной охват в подготовленных городах и регионах ---
print("\n🔍 Шаг 1: Основной охват в подготовленных городах и регионах...")
for search_text in search_texts:
    for area_id in cMillioners_extended:
        vacancies_batch = get_all_vacancies_for_params(area_id, search_text)
        for v in vacancies_batch:
            if v["id"] not in seen_ids:
                all_vacancies.append(v)
                seen_ids.add(v["id"])

        # --- НОВОЕ: Проверка на 2000 и добавление в hot_areas ---
        if len(vacancies_batch) == 2000:
            hot_areas.add(area_id)
        # --- КОНЕЦ НОВОГО ---


# --- Шаг 1.5: "Докопка" в "горячих" регионах из Шага 1 ---
print("\n🔍 Шаг 1.5: Докопка в 'горячих' регионах из Шага 1...")
for area_id in hot_areas:
    print(f"  Обработка 'горячего' региона: area_id={area_id}")
    for search_text in search_texts:
        print(f"    Разбиение по времени для '{search_text}' в area_id={area_id}...")
        current_start = datetime(2024, 1, 1)
        end_date = datetime.today()
        delta = timedelta(days=7)

        while current_start < end_date:
            current_end = min(current_start + delta - timedelta(days=1), end_date)

            date_from_str = current_start.strftime("%Y-%m-%d")
            date_to_str = current_end.strftime("%Y-%m-%d")

            print(f"      Разбиение по времени: {date_from_str} - {date_to_str} для area_id={area_id}")
            time_batch = get_all_vacancies_for_params(area_id, search_text, date_from=date_from_str, date_to=date_to_str)

            for v in time_batch:
                if v["id"] not in seen_ids:
                    all_vacancies.append(v)
                    seen_ids.add(v["id"])

            current_start += delta


# --- Шаг 2: "Докопка" в "горячих" регионах (Россия целиком) через разбиение по времени ---
print("\n🔍 Шаг 2: Проверка необходимости 'докопки' по времени для area=113...")
for search_text in search_texts:
    print(f"  Проверка общего запроса для '{search_text}'...")
    # Выполняем общий запрос для всей России
    general_batch = get_all_vacancies_for_params(113, search_text)
    
    if len(general_batch) == 2000:
        print(f"    -> Найдено ровно 2000 вакансий для '{search_text}'. Запускаем разбиение по времени...")
        
        # Определяем период
        start_date = datetime(2024, 1, 1)
        end_date = datetime.today() # или datetime(2024, 10, 23), если фиксированная дата важна
        delta = timedelta(days=7)

        current_start = start_date
        while current_start < end_date:
            current_end = min(current_start + delta - timedelta(days=1), end_date)
            
            date_from_str = current_start.strftime("%Y-%m-%d")
            date_to_str = current_end.strftime("%Y-%m-%d")
            
            print(f"      Разбиение по времени: {date_from_str} - {date_to_str}")
            time_batch = get_all_vacancies_for_params(113, search_text, date_from=date_from_str, date_to=date_to_str)
            
            for v in time_batch:
                if v["id"] not in seen_ids:
                    all_vacancies.append(v)
                    seen_ids.add(v["id"])
            
            current_start += delta
    else:
        print(f"    -> Найдено {len(general_batch)} вакансий для '{search_text}'. Разбиение по времени НЕ требуется.")


# --- Шаг 3: Охват "остальных" вакансий, опубликованных с area=113 (вся Россия) ---
# (Только если разбиение по времени НЕ запускалось)
for search_text in search_texts:
    # Проверяем, запускалось ли разбиение для этого search_text
    # (Можно было бы сделать более явную проверку, сохранив флаг)
    # Проверим общее количество вакансий в general_batch для этого search_text
    # Но проще один раз выполнить финальный общий запрос area=113 и отфильтровать,
    # чтобы не усложнять логику, если мы уже всё собрали через разбиение.
    # Однако, если разбиение НЕ запускалось, этот шаг критически важен.
    # Для простоты, мы выполним его всегда, но отфильтруем по seen_ids.
    # Это не добавит новых вакансий, если разбиение сработало, но обеспечит полноту, если не сработало.
    print(f"\n🔍 Шаг 3: Финальная проверка area=113 для '{search_text}' (для оставшихся вакансий, если разбиение не сработало)...")

    url = "https://api.hh.ru/vacancies"
    params = {
        "text": search_text,
        "area": 113, # Вся Россия
        "per_page": 100,
        "page": 0,
        "search_field": "name"
    }

    extra_batch = []
    page = 0
    max_pages = 20

    while page < max_pages:
        params["page"] = page
        response = requests.get(url, headers=headers, params=params)
        
        if response.status_code != 200:
            print(f"    Ошибка на странице {page} при area=113, text='{search_text}': {response.status_code}")
            break
            
        data = response.json()
        vacancies = data["items"]
        if not vacancies:
            break

        # Фильтруем:
        # 1. area_id не в cMillioners_extended (чтобы не дублировать Шаг 1)
        # 2. id не в seen_ids
        # 3. id существует в вакансии
        for v in vacancies:
            v_id = v.get("id")
            v_area_id = v.get("area_id")
            if v_area_id not in cMillioners_extended and v_id is not None and v_id not in seen_ids:
                all_vacancies.append(v)
                seen_ids.add(v_id)
                extra_batch.append(v)

        print(f"    Страница {page} (area=113, text='{search_text}'): получено {len(vacancies)}, добавлено (после фильтрации по area и id): {len([v for v in vacancies if v.get('area_id') not in cMillioners_extended and v.get('id') is not None and v.get('id') not in seen_ids])}")
        page += 1
        time.sleep(0.5)

    print(f"    ✅ Добавлено вакансий из 'остальных' (area=113, фильтр по area и id) для '{search_text}': {len(extra_batch)}")


# --- Шаг 4: Финальная очистка (уже частично сделана через seen_ids) ---
print(f"\n✅ Уникальных вакансий после всех шагов: {len(all_vacancies)}")

# Преобразуем в DataFrame
flattened = [flatten_vacancy(v) for v in all_vacancies]
df = pd.DataFrame(flattened)

print(df.head())

FinalDF = df.pipe(lambda x: x
    .astype({
        "salary_from": "Int64",
        "salary_to": "Int64", 
        "id": "int64"
    })
    .drop(columns=['area_id', 'employer_id', 'role_id'])
    .drop_duplicates()
    .assign(salary_gross=lambda x: x["salary_gross"].map({
        True: True,
        False: False,
        "true": True, 
        "false": False,
        None: None
    }).astype("boolean"))
)


supabase = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_KEY"])
# Подготовка записей
records = FinalDF.to_dict(orient="records")

# Загрузка с upsert по id
try:
    supabase.table("hh_vacancies").upsert(records, on_conflict="id").execute()
    print(f"✅ Успешно загружено {len(records)} вакансий в Supabase")
except Exception as e:
    print(f"❌ Ошибка при загрузке: {e}")
