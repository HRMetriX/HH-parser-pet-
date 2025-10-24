import requests
from supabase import create_client
import os

# === Настройки ===
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# === Сбор всех городов с координатами ===
all_cities = []

def extract_cities_recursive(areas, parent_name=None):
    for area in areas:
        area_id = area["id"]
        area_name = area["name"]
        lat = area.get("lat")
        lng = area.get("lng")
        sub_areas = area.get("areas", [])
        
        if not sub_areas and lat is not None and lng is not None:
            # Это город с координатами
            all_cities.append({
                "area_name": area_name,
                "lat": lat,
                "lng": lng,
                "region_name": parent_name
            })
        elif sub_areas:
            # Это регион — рекурсивно идём вглубь
            extract_cities_recursive(sub_areas, parent_name=area_name)

# === Основной код ===
print("🔍 Загружаем иерархию регионов и городов из hh.ru...")
response = requests.get("https://api.hh.ru/areas/113")
if response.status_code != 200:
    raise Exception(f"Ошибка загрузки регионов: {response.status_code}")

russia_data = response.json()
extract_cities_recursive(russia_data["areas"])

print(f"✅ Найдено {len(all_cities)} городов с координатами")

# === Подготовка записей ===
records = []
for city in all_cities:
    lat = city["lat"]
    lng = city["lng"]
    city_id = f"{lat:.6f}_{lng:.6f}"
    records.append({
        "id": city_id,
        "area_name": city["area_name"],
        "lat": lat,
        "lng": lng,
        "region_name": city["region_name"]
    })

# === Загрузка в Supabase ===
if records:
    supabase.table("city_geo").upsert(records, on_conflict="id").execute()
    print(f"\n✅ Успешно сохранено {len(records)} городов в city_geo")
else:
    print("📭 Нет данных для сохранения")
