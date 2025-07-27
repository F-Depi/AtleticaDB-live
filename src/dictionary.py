import pandas as pd
import csv
import json
import os
import re
from functions_general import hard_strip
from assegnazione_evento import assegna_evento_generale, assegna_evento_specifico

## File utilizzato per creare la prima versione del dizionario. Ora serve solo ad aggiornare con le cose nuove che escono da run_me.py

""" file_dizionario = 'Generale/event_dict.json'
file_dizionario_new = 'Generale/event_dict_new.json'

def merge_dict(file1, file2):
    
    ## funzione per unire 2 file .json
    ## gli elementi in file2 vengono copiari in file1 e file2 viene eliminato
    
    with open(file1, 'r') as f1:
        dict1 = json.load(f1)

    with open(file2, 'r') as f2:
        dict2 = json.load(f2)

    # Merge the dictionaries
    dict1.update(dict2)
    
    with open(file1, 'w') as f3:
        json.dump(dict1, f3, sort_keys=True, indent=4)
    
    os.remove(file2)
    
    return """

#merge_dict(file_dizionario, file_dizionario_new)

## Dizionario per tradurre i nomi vari che vengono dati agli eventi (es '60Hs Donne', '60 Hs H84 J/P/S Donne', '60Hs H84-8.50 JUNIORES-PROMESSE-SENIORES FEMMINILI')
## in un nome standard che indetifichi univocamente la disciplina (es '60 Hs h84-8.50')
## Sono sostanzialmente fatti a mano guardando tutti quelli che get_results_links ha trovato nelle gare.


discipline_all = []
nomi_all = []

for X in range(19, 25):
    file_20XX = 'indoor_20'+str(X)+'/link_risultati_key.csv'
    with open(file_20XX, 'r', newline='') as f1: #apro il dizionario
        reader = csv.reader(f1)
        for row in reader:
            disciplina = row[2]
            nome = row[3]
            discipline_all.append(disciplina)
            nomi_all.append(nome)
            
print(str(len(nomi_all)) + '\teventi totali dal 2019 al 2024')

dumb_dict = {}
nomi_diversi = []
for disciplina, nome in zip(discipline_all, nomi_all):
    if nome not in dumb_dict:
        dumb_dict[nome] = [disciplina]
        nomi_diversi.append([nome, disciplina])

print(str(len(dumb_dict)) + '\teventi con nome unico dal 2019 al 2024')

nomi_diversi = nomi_diversi[1:] # header via
df_nomi = pd.DataFrame(nomi_diversi, columns=['Nome','Evento Specifico Vecchio'])
df_nomi[['Evento Generale','Evento Specifico','Warning Generale','Warning Specifico']] = ''
df_nomi = df_nomi[['Nome','Evento Generale','Warning Generale','Evento Specifico','Warning Specifico','Evento Specifico Vecchio']]

for ii, row  in df_nomi.iterrows():
    
    nome = df_nomi.loc[ii,'Nome']
    eve_gen = ''
    warn_gen = ''
    eve_spec = ''
    eve_gen = ''
    
    (eve_gen, warn_gen) = assegna_evento_generale(nome)
    (eve_spec, warn_spec) = assegna_evento_specifico(nome, eve_gen)
    
    df_nomi.loc[ii,'Evento Generale'] = eve_gen
    df_nomi.loc[ii,'Warning Generale'] = warn_gen
    df_nomi.loc[ii,'Evento Specifico'] = eve_spec
    df_nomi.loc[ii,'Warning Specifico'] = warn_spec
    

df_nomi.to_csv('tutti.csv',index=False)

    

    

"""
# Assegnamo prima un evento generale:
# 'altro','peso','disco','martello','giavellotto','pallina','palla','vortex','asta','lungo da fermo'
# 'lungo','alto','triplo','quadruplo','ostacoli','marcia','staffetta','corsa piana','prove multiple','boh'

for ii, (n, d) in enumerate(nomi_diversi):
    
    ## dato un nome di un evento n, come appare nella pagina della gara, gli assegna una categoria generale:
    ## 'altro','peso','disco','martello','giavellotto','pallina','palla','vortex','asta','lungo da fermo'
    ## 'lungo','alto','triplo','quadruplo','ostacoli','marcia','staffetta','corsa piana','prove multiple','boh'
    ## restituisce il nome della categoria e un'altra stringa con i possibili warning
    
    n = n.lower().replace('finale','').strip()
    check = 0
    
    # Il + spesso compare quando ci sono due disciplina messe assieme.Ma se compare alla fine di solito è per fare riferimento all'età dei master.
    # Se compare più di una volta di solito è perchè concatenano le categorie con il +
    if n[:-5].count('+') == 1:
        df_nomi.loc[ii,'Warning'] = '\'+\' sus'
    
    # ALTRO
    for word in ['modello','classifica','complessiv','completi','risultati','1/sta','1 sta','1-sta','1sta','statistica','somma tempi','premio']:  
        if word in n:
            df_nomi.loc[ii,'Evento Generale'] = 'altro'
            check = check + 1
            break
    
    # LANCI
    for word in ['peso','disco','martello','giavellotto','pallina','palla','vortex']:
        if word in n:
            df_nomi.loc[ii,'Evento Generale'] = word
            check = check + 1
            break
    
    # SALTI
    for word in ['asta','lungo da fermo','lungo','alto','triplo','quadruplo']:
        if word in n:
            df_nomi.loc[ii,'Evento Generale'] = word
            check = check + 1
            break
    if 'high jump' in n:
        df_nomi.loc[ii,'Evento Generale'] = 'alto'
        check = check + 1
    if 'long jump' in n:
        df_nomi.loc[ii,'Evento Generale'] = 'lungo'
        check = check + 1
    if 'triple jump' in n:
        df_nomi.loc[ii,'Evento Generale'] = 'triplo'
        check = check + 1
    if 'pole vault' in n:
        df_nomi.loc[ii,'Evento Generale'] = 'asta'
        check = check + 1
    if n.startswith('pv'):
        df_nomi.loc[ii,'Evento Generale'] = 'asta'
        check = check + 1
        

    # OSTACOLI
    ostacoli = ['ostacoli',' hs ','hurdle']
    for word in ostacoli:
        if word in n:
            df_nomi.loc[ii,'Evento Generale'] = 'ostacoli'
            check = check + 1
    
    pattern_hs = r'\d+hs'
    match_hs = re.search(pattern_hs, n)
    if match_hs:
        df_nomi.loc[ii,'Evento Generale'] = 'ostacoli'
        check = check + 1
        
    ## MARCIA
    if ('marcia' in n) | ('race walking' in n):
        df_nomi.loc[ii,'Evento Generale'] = 'marcia'
        check = check + 1
        
    ## STAFFETTA
    if ('staffetta' in n) | ('staff.' in n) | ('relay' in n) | (n[1] == 'x'):
        
        df_nomi.loc[ii,'Evento Generale'] = 'staffetta'
        check = check + 1

        if 'giro' in n:
            n = n.replace('1giro','200').replace('1 giro','200')
        if 'giri' in n:
            n = n.replace('2giri','400').replace('2 giri','400')
        pattern_staff = r'\b\d+x\d+\b'
        pattern_staff2 = r'\b\d+ x \d+\b'
        
        match_staff = re.findall(pattern_staff, n)
        if match_staff:
            df_nomi.loc[ii,'Evento Specifico'] = re.findall(pattern_staff, n)[0].strip().replace(' ','') + 'm'
        match_staff2 = re.findall(pattern_staff2, n)
        if match_staff2:
            df_nomi.loc[ii,'Evento Specifico'] = re.findall(pattern_staff2, n)[0].strip().replace(' ','') + 'm'
    
    
    # Dopo tutto questo dovrei aver pulito abbastanza i nomi da poter fare
    # CORSE
    if check == 0:
        pattern_corse1 = r'^\d+' # assumo che le corse abbiano sempre la distanza all'inizio
        match_corse1 = re.findall(pattern_corse1, n)
        if match_corse1:
            df_nomi.loc[ii,'Evento Generale'] = 'corsa piana'
            check = check + 1
            df_nomi.loc[ii,'Evento Specifico'] = match_corse1[0].strip() + 'm'

    # PROVE MULTIPLE
    if check == 0:
        # a volte le corse delle multiple hanno 'multiple - 800m', quindi se c'è un numero potrebbe essere una corsa.
        if any(char.isdigit() for char in n):
            df_nomi.loc[ii,'Warning'] = (df_nomi.loc[ii,'Warning'] + ' ' + 'PM sus').strip()
        if 'thlon' in n:
            df_nomi.loc[ii,'Evento Generale'] = 'prove multiple'
            check = check + 1
        
    # siepi
        
    if check > 1:
        df_nomi.loc[ii,'Warning'] = (df_nomi.loc[ii,'Warning'] + ' ' + str(check) + ' if').strip()
    
    # Sperando non sia rimasto nulla (falso ho trascurato le siepi, ma siamo indoor per ora)
    if check == 0:
        df_nomi.loc[ii,'Evento Generale'] = 'boh'

 """

