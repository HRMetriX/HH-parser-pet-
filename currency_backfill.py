import asyncio
import aiohttp
import xmltodict
from datetime import datetime, timedelta
from supabase import create_client
import os
from datetime import datetime, timezone # Добавлено для исправления предупреждения

# === Настройки ===
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

async def fetch_currency_rate(session, date_str):
    """
    Асинхронно получает курсы для одной даты.
    date_str: строка в формате DD/MM/YYYY
    """
    url = f"https://www.cbr.ru/scripts/XML_daily.asp?date_req={date_str}"
    try:
        async with session.get(url) as response:
            if response.status != 200:
                print(f"⚠️ Пропуск {date_str}: статус {response.status}")
                # Даже при ошибке API, возвращаем запись с None, но с правильным форматом даты для upsert
                # Преобразуем DD/MM/YYYY в YYYY-MM-DD
                try:
                    date_obj = datetime.strptime(date_str, "%d/%m/%Y")
                    iso_date_str = date_obj.strftime("%Y-%m-%d")
                except ValueError:
                    # На всякий случай, если формат date_str неожиданно изменится
                    print(f"❌ Ошибка преобразования даты {date_str}")
                    return {"date": date_str, "usd_to_rub": None, "eur_to_rub": None}
                return {"date": iso_date_str, "usd_to_rub": None, "eur_to_rub": None}

            content = await response.text()
            data = xmltodict.parse(content)
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

            # Преобразуем DD/MM/YYYY в YYYY-MM-DD перед сохранением
            try:
                date_obj = datetime.strptime(date_str, "%d/%m/%Y")
                iso_date_str = date_obj.strftime("%Y-%m-%d")
            except ValueError:
                # На всякий случай, если формат date_str неожиданно изменится
                print(f"❌ Ошибка преобразования даты {date_str}")
                return {"date": date_str, "usd_to_rub": usd_rate, "eur_to_rub": eur_rate}
            
            print(f"✅ {iso_date_str}: USD={usd_rate}, EUR={eur_rate}") # В логе теперь отображается ISO формат
            return {"date": iso_date_str, "usd_to_rub": usd_rate, "eur_to_rub": eur_rate}

    except Exception as e:
        print(f"❌ Ошибка при загрузке {date_str}: {e}")
        # Даже при исключении, пытаемся вернуть правильный формат даты
        try:
            date_obj = datetime.strptime(date_str, "%d/%m/%Y")
            iso_date_str = date_obj.strftime("%Y-%m-%d")
        except ValueError:
            # Если и преобразование даты не удалось, возвращаем как есть (может вызвать ошибку в Supabase)
            print(f"❌ Ошибка преобразования даты {date_str} внутри исключения")
            return {"date": date_str, "usd_to_rub": None, "eur_to_rub": None}
        return {"date": iso_date_str, "usd_to_rub": None, "eur_to_rub": None}

async def main():
    # === Период загрузки ===
    # Используем timezone-aware datetime для исправления предупреждения
    start_date = datetime(2024, 1, 1, tzinfo=timezone.utc)
    # end_date = datetime.now(timezone.utc) - timedelta(days=1)  # вчера в UTC
    # Для совместимости с предыдущим кодом, если нужна локальная дата:
    # from datetime import date
    # end_date = date.today() - timedelta(days=1)
    # Но проще использовать UTC:
    end_date = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)  # вчера 00:00 UTC
    print(f"Загружаем курсы с {start_date.date()} по {end_date.date()}")

    # Генерируем список всех дат
    date_list = []
    current_date = start_date
    while current_date.date() <= end_date.date():
        date_list.append(current_date.strftime("%d/%m/%Y"))
        current_date += timedelta(days=1)

    # Создаём сессию aiohttp с таймаутом
    timeout = aiohttp.ClientTimeout(total=15)  # Увеличенный таймаут
    async with aiohttp.ClientSession(timeout=timeout) as session:
        # Создаём список задач
        tasks = [fetch_currency_rate(session, date_str) for date_str in date_list]
        
        # Выполняем все задачи параллельно
        results = await asyncio.gather(*tasks)

    # === Загрузка в Supabase ===
    if results:
        try:
            # upsert по колонке 'date'
            supabase.table("currency_rates").upsert(results, on_conflict="date").execute()
            print(f"\n✅ Успешно загружено {len(results)} записей в currency_rates")
        except Exception as e:
            print(f"❌ Ошибка загрузки в Supabase: {e}")
    else:
        print(" moss Нет данных для загрузки")

# Запускаем асинхронную функцию с помощью await
await main() # <--- Вот изменение!
