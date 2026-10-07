import sys
from datetime import datetime

from func_general import (
    get_db_engine,
    get_events_link,
    get_meet_info,
    update_gare_database,
)
from func_scrape import gare_in_DB, get_iscritti


""" 1. Cerca nuove gare nel calendario """
print("Scarico l'elenco delle gare dal calendario Fidal (tabella 'gare')")
anno = datetime.now().year
anno = 2022
for tipo in ["3", "5", "10"]:
   update_gare_database(str(anno), mese="", regione="", categoria="", tipo=tipo)


""" 2. Aggiorna versione sigma e status delle gare """
print("-" * 30)
print("Ottengo informazioni su ogni gara (aggiorno 'gare')")

with get_db_engine().connect() as conn:
    get_meet_info(conn, "date_1")


""" 3. Trova i link alle singole gare (tabella 'pagine_gara') """
print("\n---------------------------------------------")
print("Cerco i link a iscritti/risultati di ogni disciplina (aggiorno 'pagine_gara')")
with get_db_engine().connect() as conn:
    get_events_link(conn, "date_1")


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
