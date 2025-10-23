import requests
import xmltodict
from datetime import datetime, timedelta, timezone
from supabase import create_client
import os

# === Настройки ===
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# === Дата: вчера в UTC → преобразуем в дату без времени ===
yesterday_utc = datetime.now(timezone.utc) - timedelta(days=1)
yesterday_date = yesterday_utc.date()  # например: 2025-10-22
date_str_api = yesterday_date.strftime("%d/%m/%Y")  # формат для ЦБ РФ: 22/10/2025

print(f"Загружаем курсы валют за дату: {yesterday_date}")

# === Запрос к ЦБ РФ ===
url = f"https://www.cbr.ru/scripts/XML_daily.asp?date_req={date_str_api}"
try:
    response = requests.get(url, timeout=10)
    if response.status_code != 200:
        print(f"❌ Ошибка API ЦБ РФ: статус {response.status_code}")
        exit(1)

    # Парсинг XML
    data = xmltodict.parse(response.content)
    valutes = data.get("ValCurs", {}).get("Valute", [])
    
    usd_rate = None
    eur_rate = None

    for v in valutes:
        char_code = v.get("CharCode")
        value = v.get("Value")
        if value:
            value = float(value.replace(",", "."))
            if char_code == "USD":
                usd_rate = value
            elif char_code == "EUR":
                eur_rate = value

    print(f"✅ Получены курсы: USD={usd_rate}, EUR={eur_rate}")

    # === Подготовка записи ===
    record = {
        "date": yesterday_date.isoformat(),  # "2025-10-22"
        "usd_to_rub": usd_rate,
        "eur_to_rub": eur_rate
    }

    # === Загрузка в Supabase ===
    supabase.table("currency_rates").upsert([record], on_conflict="date").execute()
    print(f"✅ Курсы за {yesterday_date} успешно сохранены в Supabase")

except Exception as e:
    print(f"❌ Ошибка при загрузке курсов: {e}")
    exit(1)
