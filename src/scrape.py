from func_general import get_db_engine
from func_scrape import get_iscritti, gare_in_DB


""" Scarichiamo tutti gli iscritti alle gare """
update_condition = 'date_0'
with get_db_engine().connect() as conn:
    get_iscritti(conn, update_condition)


""" Scarichiamo tutti i risultati alle gare """
# NO


"""
Cataloghiamo le gara con True/False se i risultati sono stati caricati nel
database FIDAL e sono quindi nella tabella results oppure no.
"""
update_condition = 'date_7'
with get_db_engine().connect() as conn:
    gare_in_DB(conn, update_condition)
