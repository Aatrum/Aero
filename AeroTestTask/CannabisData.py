import requests
import json
import psycopg2

from Config import cAddress

from Config import pgDatabase
from Config import pgHost
from Config import pgUser
from Config import pgPassword
from Config import pgPort

conn = None

# Получение данных
def GetData(url):
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json"
    } 
    response = requests.get(url)

    if response.status_code == 200:
        return json.loads(response.text)
    else:
        return None

# Подключение к БД
def DBConnect():
    global conn
    conn = psycopg2.connect(database=pgDatabase,
                        host=pgHost,
                        user=pgUser,
                        password=pgPassword,
                        port=pgPort)
    
    return conn.cursor()

# Структура таблицы для импорта
"""
CREATE TABLE IF NOT EXISTS public.random_cannabis
(
    id bigint NOT NULL,
    uid uuid NOT NULL,
    strain text COLLATE pg_catalog."default",
    cannabinoid_abbreviation text COLLATE pg_catalog."default",
    cannabinoid text COLLATE pg_catalog."default",
    terpene text COLLATE pg_catalog."default",
    medical_use text COLLATE pg_catalog."default",
    health_benefit text COLLATE pg_catalog."default",
    category text COLLATE pg_catalog."default",
    type text COLLATE pg_catalog."default",
    buzzword text COLLATE pg_catalog."default",
    brand text COLLATE pg_catalog."default",
    CONSTRAINT random_cannabis_pkey PRIMARY KEY (uid)
)
"""

# Импорт в БД
def DBImport(cursor, data):
    global conn
    sql_query = """
    INSERT INTO public.random_cannabis
    select * from json_populate_recordset(NULL::random_cannabis, %s) 
    on conflict (uid)
	do update set (id, strain, cannabinoid_abbreviation, cannabinoid, terpene, medical_use, health_benefit, category, "type", buzzword, brand) = 
	(excluded.id, excluded.strain, excluded.cannabinoid_abbreviation, excluded.cannabinoid, excluded.terpene, excluded.medical_use, excluded.health_benefit, excluded.category, excluded."type", excluded.buzzword, excluded.brand)
    """

    cursor.execute(sql_query, (json.dumps(data),))
    conn.commit() 


data = GetData(cAddress)
curs = DBConnect()

if ((data != None) and not(curs.closed)):
    DBImport(curs, data)