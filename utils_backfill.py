import requests
import pandas as pd
import time
from datetime import datetime, timedelta
from supabase import create_client
import os
import supabase

# Заголовок — обязательно с email!
headers = {
    "User-Agent": "MemeWeather-HH-Pipeline/1.0 (oborisov.personal@gmail.com)"
}

def flatten_vacancy(v):
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
    return all_vacancies
