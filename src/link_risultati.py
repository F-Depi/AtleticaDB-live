import pandas as pd
import os
from func_general import update_gare_database, get_meet_info, get_events_link, get_db_engine
from func_assegnazione_evento import assegna_evento_generale, assegna_evento_specifico
import time
start_time = time.time()


""" Cerca nuove fare nel calendario """
print('\n---------------------------------------------')
print("Scarico l'elenco delle gare dal calendario Fidal")

anno = '2025'
mese = ''
regione = ''
categoria = ''
for tipo in ['3', '5']:
    update_gare_database(str(anno), mese, regione, categoria, tipo)


""" Aggiorna le informazioni sulle gare """
print('\n---------------------------------------------')
print("Ottengo informazioni su ogni gara")

update_condition = 'null' # righe appena aggiunte
with get_db_engine().connect() as conn:
    get_meet_info(conn, update_condition)

update_condition = 'date_7' # routine update
with get_db_engine().connect() as conn:
    get_meet_info(conn, update_condition)



""" Otteniamo i link a ogni risultato di ogni disciplina per ogni gara """
## per ora ci occupiamo solo di trovare 'Nome' e 'Link'
## get_events_link() prende link_risultati per ogni gara e salva tutti i link
## che trova in quella pagina 
## NOTA: il criterio di aggiornamento è solo quello di aggiornare gare finite da meno di N giorni.
## Quindi gare più vecchie non vengono aggiornate, nel bene o nel male. Dovrei aggiungere un'altra
## colonna con 'Ultimo Aggiornamento', ma non oggi.
print('\n---------------------------------------------')
print("Ora cerco i link agli eventi di ogni gara")

update_condition = 'date_7'
#update_condition = 'ok'
#update_condition = 'scrape_60'
with get_db_engine().connect() as conn:
    get_events_link(conn, update_condition)


exit()
################# Identifichiamo la disciplina corretta con il dizionari dei nomi #################
## molto del lavoro sul dizionario è stato inizialmente fatto a mano, poi gestito in dizionario.py
## I nomi dati alle discipline, che variano in base a quello che sceglie l'organizzatore della gara,
## vengono "puliti" dalla funzione hard_strip() per ridurre le dimensioni del dizionario.
## Il rischio di errore in questa parte è alto a causa di typo miei, nomi molto ambigui, hard_strip()
## che incontra un nome così strano che tolta qualche lettera diventa una disciplina diversa (ho fatto
## in modo che questa cosa sia improbabile)
""" print('---------------------------------------------')
print('Applico il dizionario per dare il nome corretto agli eventi')

with open(file_dizionario, 'r') as f1:
    event_dict = json.load(f1)

eventi_ignoti = []
for ii, row in df_risultati.iterrows():

    nome = row['Nome']
    nome = hard_strip(nome)

    if nome in event_dict:
        df_risultati.loc[ii, 'Disciplina'] = event_dict[nome]
    else:
        print('\nNon conosco ' + nome)
        eventi_ignoti.append([nome, 'boh'])

df_risultati.to_csv(file_risultati_key, index=False)

if eventi_ignoti:
    with open(file_dizionario_new, 'w') as f2:
        f2.write('Nome,Disciplina\n')
        for a, b in eventi_ignoti:
            f2.write(a+','+b+'\n') """


for ii, row  in df_risultati.iterrows():

    nome = df_risultati.loc[ii,'Nome']
    disciplina = df_risultati.loc[ii,'Disciplina']
    link = df_risultati.loc[ii, 'Link']
    nome = str(nome)
    disciplina = str(disciplina).strip()
    link = str(link)

    #if disciplina == 'boh':

    eve_gen = ''
    warn_gen = ''
    eve_spec = ''
    eve_gen = ''

    eve_gen, warn_gen = assegna_evento_generale(nome, link)
    eve_spec, warn_spec = assegna_evento_specifico(nome, eve_gen)

    df_risultati.loc[ii,'Disciplina'] = eve_spec
    df_risultati.loc[ii,'Warning'] = (warn_gen+' '+warn_spec).strip()


print("--- %s secondi ---" % round(time.time() - start_time, 2))
df_risultati.to_csv(file_risultati, index=False)
