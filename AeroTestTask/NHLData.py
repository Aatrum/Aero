import requests
import json
import psycopg2
import re

from Config import nAddress
from Config import nTeamNum

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

# Структура таблицы statsSingleSeason для импорта
"""
CREATE TABLE IF NOT EXISTS public.statssingleseason
(
    id smallint NOT NULL,
    name text COLLATE pg_catalog."default" NOT NULL,
    gamesplayed smallint,
    wins smallint,
    losses smallint,
    ot smallint,
    pts smallint,
    ptpctg real,
    goalspergame real,
    goalsagainstpergame real,
    evggaratio real,
    powerplaypercentage real,
    powerplaygoals real,
    powerplaygoalsagainst real,
    powerplayopportunities real,
    penaltykillpercentage real,
    shotspergame real,
    shotsallowed real,
    winscorefirst real,
    winoppscorefirst real,
    winleadfirstper real,
    winleadsecondper real,
    winoutshootopp real,
    winoutshotbyopp real,
    faceoffstaken real,
    faceoffswon real,
    faceoffslost real,
    faceoffwinpercentage real,
    shootingpctg real,
    savepctg real,
    CONSTRAINT statssingleseason_pkey PRIMARY KEY (id)
)
"""

# Импорт в БД в таблицу statsSingleSeason
def DBImportStat(cursor, data):
    global conn
    sql_query = """
    INSERT INTO public.statsSingleSeason
    select * from json_populate_record(NULL::statsSingleSeason, %s) 
    on conflict (id)
	do update set (name, gamesplayed, wins, losses, ot, pts, ptpctg, goalspergame, goalsagainstpergame, evggaratio, powerplaypercentage, powerplaygoals, powerplaygoalsagainst, powerplayopportunities, penaltykillpercentage, shotspergame, shotsallowed, winscorefirst, winoppscorefirst, winleadfirstper, winleadsecondper, winoutshootopp, winoutshotbyopp, faceoffstaken, faceoffswon, faceoffslost, faceoffwinpercentage, shootingpctg, savepctg) = 
	(excluded.name, excluded.gamesplayed, excluded.wins, excluded.losses, excluded.ot, excluded.pts, excluded.ptpctg, excluded.goalspergame, excluded.goalsagainstpergame, excluded.evggaratio, excluded.powerplaypercentage, excluded.powerplaygoals, excluded.powerplaygoalsagainst, excluded.powerplayopportunities, excluded.penaltykillpercentage, excluded.shotspergame, excluded.shotsallowed, excluded.winscorefirst, excluded.winoppscorefirst, excluded.winleadfirstper, excluded.winleadsecondper, excluded.winoutshootopp, excluded.winoutshotbyopp, excluded.faceoffstaken, excluded.faceoffswon, excluded.faceoffslost, excluded.faceoffwinpercentage, excluded.shootingpctg, excluded.savepctg)
    """

    cursor.execute(sql_query, (json.dumps(data),))
    conn.commit() 

# Структура таблицы regularseasonstatrankings для импорта
"""
CREATE TABLE IF NOT EXISTS public.regularseasonstatrankings
(
    id smallint NOT NULL,
    name text COLLATE pg_catalog."default" NOT NULL,
    wins smallint,
    losses smallint,
    ot smallint,
    pts smallint,
    ptpctg smallint,
    goalspergame smallint,
    goalsagainstpergame smallint,
    evggaratio smallint,
    powerplaypercentage smallint,
    powerplaygoals smallint,
    powerplaygoalsagainst smallint,
    powerplayopportunities smallint,
    penaltykillopportunities smallint,
    penaltykillpercentage smallint,
    shotspergame smallint,
    shotsallowed smallint,
    winscorefirst smallint,
    winoppscorefirst smallint,
    winleadfirstper smallint,
    winleadsecondper smallint,
    winoutshootopp smallint,
    winoutshotbyopp smallint,
    faceoffstaken smallint,
    faceoffswon smallint,
    faceoffslost smallint,
    faceoffwinpercentage smallint,
    savepctrank smallint,
    shootingpctrank smallint,
    CONSTRAINT regularseasonstatrankings_pkey PRIMARY KEY (id)
)
"""

# Импорт в БД
def DBImportRankings(cursor, data):
    global conn
    sql_query = """
    INSERT INTO public.regularseasonstatrankings
    select * from json_populate_record(NULL::regularseasonstatrankings, %s) 
    on conflict (id)
	do update set (name, wins, losses, ot, pts, ptpctg, goalspergame, goalsagainstpergame, evggaratio, powerplaypercentage, powerplaygoals, powerplaygoalsagainst, powerplayopportunities, penaltykillopportunities, penaltykillpercentage, shotspergame, shotsallowed, winscorefirst, winoppscorefirst, winleadfirstper, winleadsecondper, winoutshootopp, winoutshotbyopp, faceoffstaken, faceoffswon, faceoffslost, faceoffwinpercentage, shootingpctrank, savepctrank) = 
	(excluded.name, excluded.wins, excluded.losses, excluded.ot, excluded.pts, excluded.ptpctg, excluded.goalspergame, excluded.goalsagainstpergame, excluded.evggaratio, excluded.powerplaypercentage, excluded.powerplaygoals, excluded.powerplaygoalsagainst, excluded.powerplayopportunities, excluded.penaltykillopportunities, excluded.penaltykillpercentage, excluded.shotspergame, excluded.shotsallowed, excluded.winscorefirst, excluded.winoppscorefirst, excluded.winleadfirstper, excluded.winleadsecondper, excluded.winoutshootopp, excluded.winoutshotbyopp, excluded.faceoffstaken, excluded.faceoffswon, excluded.faceoffslost, excluded.faceoffwinpercentage, excluded.shootingpctrank, excluded.savepctrank)
    """

    cursor.execute(sql_query, (json.dumps(data),))
    conn.commit() 

data = GetData(nAddress+str(nTeamNum)+'/stats')
curs = DBConnect()

if ((data != None) and not(curs.closed)):
    
    for stat in data["stats"]:
        importData = {}
        if stat["type"]["displayName"] == 'statsSingleSeason':
            split = stat["splits"][0]
            
            importData['id'] = split['team']['id']
            importData['name'] = split['team']['name']

            #приводим все наименования к нижнему регистру, чтобы наименования полей JSon замаппились на поля таблицы
            importData.update(json.loads(json.dumps(split['stat']).lower()))

            DBImportStat(curs, importData)
        
        if stat["type"]["displayName"] == 'regularSeasonStatRankings':
            split = stat["splits"][0]
            
            importData['id'] = split['team']['id']
            importData['name'] = split['team']['name']

            # Приводим все наименования к нижнему регистру, чтобы наименования полей JSon замаппились на поля таблицы
            strData = json.dumps(split['stat']).lower()
            # Конвертируем все числительные в числа
            strData = re.sub(r"(\d)(\D\D)", r"\1", strData)

            importData.update(json.loads(strData))

            DBImportRankings(curs, importData)

