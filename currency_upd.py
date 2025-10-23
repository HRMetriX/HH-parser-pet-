import requests
import xmltodict
from datetime import datetime, timedelta, timezone
from supabase import create_client
import os
import traceback # Для вывода стека вызовов

# === Настройки ===
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
if not SUPABASE_URL or not SUPABASE_KEY:
    print("❌ Ошибка: Переменные окружения SUPABASE_URL или SUPABASE_KEY не заданы.")
    exit(1)

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# === Дата: вчера в UTC → преобразуем в дату без времени ===
yesterday_utc = datetime.now(timezone.utc) - timedelta(days=1)
yesterday_date = yesterday_utc.date()  # например: 2025-10-22
date_str_api = yesterday_date.strftime("%d/%m/%Y")  # формат для ЦБ РФ: 22/10/2025

print(f"Загружаем курсы валют за дату: {yesterday_date}")

# === Запрос к ЦБ РФ ===
url = f"https://www.cbr.ru/scripts/XML_daily.asp?date_req={date_str_api}"
try:
    print(f"  -> Отправка запроса к {url}...")
    response = requests.get(url, timeout=30) # Увеличенный таймаут до 30 секунд
    print(f"  <- Получен ответ с кодом: {response.status_code}")
    
    if response.status_code != 200:
        print(f"❌ Ошибка API ЦБ РФ: статус {response.status_code}")
        print(f"   Тело ответа (если есть): {response.text[:500]}...") # Показываем начало тела ответа
        exit(1)

    # Парсинг XML
    print("  -> Парсинг XML ответа...")
    data = xmltodict.parse(response.content)
    print("  <- XML успешно разобран.")
    
    valutes = data.get("ValCurs", {}).get("Valute", [])
    
    usd_rate = None
    eur_rate = None

    for v in valutes:
        char_code = v.get("CharCode")
        value = v.get("Value")
        if value:
            try:
                value = float(value.replace(",", "."))
            except ValueError:
                print(f"  ⚠️  Пропуск валидной валюты {char_code}: значение '{v.get('Value')}' не является числом.")
                continue # Переходим к следующей валюте
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
    print("  -> Отправка данных в Supabase...")
    supabase.table("currency_rates").upsert([record], on_conflict="date").execute()
    print(f"✅ Курсы за {yesterday_date} успешно сохранены в Supabase")

except requests.exceptions.Timeout:
    print(f"❌ Ошибка: Превышено время ожидания при запросе к ЦБ РФ ({url}).")
    print(f"   Таймаут был установлен на 30 секунд.")
    # Не вызываем exit(1) здесь, если хочешь, чтобы программа пыталась дальше обработать ошибку
    # Но обычно при таймауте логично завершить.
    exit(1)
except requests.exceptions.RequestException as e:
    print(f"❌ Ошибка при выполнении запроса к ЦБ РФ: {e}")
    print(f"   Тип ошибки: {type(e).__name__}")
    print(f"   URL: {url}")
    # Попробуем получить статус код, если он доступен
    if hasattr(e, 'response') and e.response is not None:
        print(f"   Код ответа (если доступен): {e.response.status_code}")
        print(f"   Тело ответа ошибки (если есть): {e.response.text[:500]}...")
    traceback.print_exc() # Печатаем стек вызовов
    exit(1)
except xmltodict.expat.ExpatError as e: # Конкретная ошибка парсинга XML
    print(f"❌ Ошибка при парсинге XML ответа от ЦБ РФ: {e}")
    print(f"   Тип ошибки: {type(e).__name__}")
    print(f"   Ответ сервера (первые 1000 символов): {response.text[:1000]}...") # Печатаем начало ответа для диагностики
    traceback.print_exc() # Печатаем стек вызовов
    exit(1)
except Exception as e: # Обработка любых других исключений
    print(f"❌ Неожиданная ошибка при загрузке курсов: {e}")
    print(f"   Тип ошибки: {type(e).__name__}")
    print(f"   URL: {url}")
    # Попробуем получить статус код и тело ответа, если они были
    if 'response' in locals() and response is not None:
        print(f"   Код ответа (если доступен): {response.status_code}")
        print(f"   Тело ответа (если доступно): {response.text[:500]}...")
    traceback.print_exc() # Печатаем стек вызовов
    exit(1)
