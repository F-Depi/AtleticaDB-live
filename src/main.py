import sys
from datetime import datetime

import pandas as pd

from func_general import (
    get_db_engine,
    get_events_link,
    get_meet_info,
    update_gare_database,
)
from func_scrape import gare_in_DB, get_iscritti

conn = get_db_engine().connect()

""" 1. Cerca nuove gare nel calendario """
print("Scarico l'elenco delle gare dal calendario Fidal (tabella 'gare')")
anno = datetime.now().year
anno_DB = pd.read_sql("""SELECT EXTRACT(YEAR FROM data_fine) AS anno FROM gare
                         ORDER BY anno DESC 
                         LIMIT 1""", conn).iloc[0,0].astype(int)
for a in range(anno_DB, anno + 1):
    for tipo in ["3", "5", "10"]:
       update_gare_database(str(a), mese="", regione="", categoria="", tipo=tipo)


""" 2. Aggiorna versione sigma e status delle gare """
print("-" * 30)
print("Ottengo informazioni su ogni gara (aggiorno 'gare')")
get_meet_info(conn, "date_5")


""" 3. Trova i link alle singole gare (tabella 'pagine_gara') """
print("\n---------------------------------------------")
print("Cerco i link a iscritti/risultati di ogni disciplina (aggiorno 'pagine_gara')")
get_events_link(conn, "date_5")


sys.exit()


""" 4. Scarica gli iscritti (tabella 'iscritti') """
print("\n---------------------------------------------")
print("Scarico gli iscritti")

with get_db_engine().connect() as conn:
    # get_iscritti(conn, "date_7")
    get_iscritti(conn, "custom", f"WHERE EXTRACT(YEAR FROM data_inizio) = {anno}")

sys.exit()


""" 5. Segna le gare i cui risultati sono già nel DB FIDAL """
print("\n---------------------------------------------")
with get_db_engine().connect() as conn:
    gare_in_DB(conn, "date_120")
