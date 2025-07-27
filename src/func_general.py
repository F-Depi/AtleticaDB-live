import pandas as pd
from bs4 import BeautifulSoup
import requests
import re
from datetime import date, datetime
from sqlalchemy import create_engine, text
from config import DB_CONFIG

def get_sqlalchemy_connection_string():
    """Generates the connection string for SQLAlchemy."""
    return f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"

def get_db_engine():
    """Create and return SQLAlchemy engine."""
    connection_string = get_sqlalchemy_connection_string()
    return create_engine(connection_string)

def update_gare_database(anno, mese='', regione='', categoria='', tipo='5'):
    """Update the gare table with new meet codes."""
    engine = get_db_engine()
    
    # Get new meet codes
    df_REG_gare = extract_meet_codes_from_calendar(anno, mese, 'REG', regione, tipo, categoria)
    df_COD_gare = extract_meet_codes_from_calendar(anno, mese, 'COD', regione, tipo, categoria)
    
    df_gare = pd.concat([df_REG_gare, df_COD_gare], ignore_index=True)
    if df_gare.empty:
        print("Both df_REG_gare and df_COD_gare is None, exiting...")
        return

    df_gare['status'] = None
    df_gare['sigma'] = None
    df_gare['livello'] = df_gare['codice'].str[:1]
    df_gare['livello'] = df_gare['livello'].replace('C', 'N')
    df_gare['link_sigma'] = None
    df_gare['link_risultati'] = None
    
    # Get existing codes from database
    with engine.connect() as conn:
        query = "SELECT codice FROM gare WHERE EXTRACT(YEAR FROM data) = %s"
        if mese:
            query += " AND EXTRACT(MONTH FROM data) = %s"
            params = (anno, mese)
        else:
            params = (anno,)
        existing_codes = pd.read_sql(query, conn, params=params)
        
        # Filter new records
        new_records = df_gare[~df_gare['codice'].isin(existing_codes['codice'])]
        
        if not new_records.empty:
            print(f"Aggiungo {len(new_records['codice'])} nuovi codici gara")
            
            # Insert new records
            new_records.to_sql('gare', conn, if_exists='append', index=False)
            conn.commit()
            print("Salvataggio completato!")

        else:
            print("Nessun nuovo codice gara da aggiungere")


def extract_meet_codes_from_calendar(anno, mese, livello, regione, tipo, categoria) -> pd.DataFrame:
    ## Scarica informazioni sulle gare presenti nel calendario fidal https://www.fidal.it/calendario.php
     # Per ogni gara scarica codice gara, data, nome, home page della gara
     # 'aggiornato' è messo di defaul al 31 marzo 1896, data simbolica per dire che i risultati
     # non sono stati aggiornati di recente
     #
     # Input: fare riferimento a Readme.txt
     # Output è un DataFramne con columns=['data', 'codice', 'aggiornato', 'nome', 'link_gara']
    
    ## Componiamo il link con i parametri del filtro
    url = 'https://www.fidal.it/calendario.php?anno='+anno+'&mese='+mese+'&livello='+livello+'&new_regione='+regione+'&new_tipo='+tipo+'&new_categoria='+categoria+'&submit=Invia'
    response = requests.get(url)

    if response.status_code == 200:
        
        soup = BeautifulSoup(response.text, 'html.parser')
        div = soup.find('div', class_='table_btm')
        
        dates = []
        meet_code = []
        nome_gara = []
        home_gara = []
        
        if div:
            ## Testo con la data del meeting 
            b_elements = div.find_all('b')

            for b in b_elements:

                if 'title' in b.attrs:
                    meet_date = b.get_text(strip=True) # il format è 31/12, 30-31/12, 31/12-01/01. Quindi mi basta prendere gli ultimi 5 caratteri
                    last_day = int(meet_date[-5:-3])
                    month = int(meet_date[-2:])
                    dates.append(date(int(anno), month, last_day))
            
            ## Link, codice e nome del meeting
            a_elements = div.find_all('a', href=True)

            for a in a_elements:
                href = a['href']
                match = re.search(fr'{livello}(\d+)', href)

                nome_gara.append(a.get_text(strip=True))
                home_gara.append(href)
                meet_code.append(match[0])
                
            df = pd.DataFrame({'data': dates, 'codice': meet_code, 'aggiornato':date(1896, 3, 31), 'nome': nome_gara, 'link_gara': home_gara}) # first modern olympics date
            
            return df


        else:
            print('No tables with class \'table_btm\' found')
            return pd.DataFrame()
        
    else:
        print("Failed to fetch the webpage. status code:", response.status_code)
        return pd.DataFrame()


def updates_DB_gara_row(row, conn):
    """
    Modify one row of the DB
    """
    sql = text("""
        UPDATE gare SET
            link_sigma = :link_sigma,
            link_risultati = :link_risultati,
            sigma = :sigma,
            status = :status,
            aggiornato = :aggiornato
        WHERE codice = :codice
    """)
    conn.execute(sql, {
        'link_sigma': row['link_sigma'],
        'link_risultati': row['link_risultati'],
        'sigma': row['sigma'],
        'status': row['status'],
        'aggiornato': row['aggiornato'],
        'codice': row['codice'],
    })
    conn.commit()


def classifica_sigma(row) -> pd.DataFrame | None:
    """
    capisce che versione di sigma viene utilizzato a una gara e restituisce
    la riga del dataframe con le informazioni
    """

    cod = row['codice']
    
    meet_year = str(row['data'].year) # serve per creare il link
    url3 = 'https://www.fidal.it/risultati/' + meet_year + '/' + cod + '/Index.htm' # link della home
    r3 = requests.get(url3).status_code
    
    if r3 == 404: # la home è comune a tutti, quindi deve esistere se esiste una pagina della gara
        row['link_sigma'] = ''
        row['link_risultati'] = ''
        row['sigma'] = ''
        row['status'] = 'Gara non esistente'
        return row
        
    elif r3 == 200: # C'è la home. Ora devo solo capire che versione di sigma c'è. Arrivato a questo punto coinsidero possibile solo che le richieste abbiamo come risposta 200 o 404.
        row['link_sigma'] = url3

        ## Vediamo se è sigma nuovo
        url1 = 'https://www.fidal.it/risultati/'+meet_year+'/' + cod + '/link_risultati/Indexlink_risultatiPerGara.html'
        r1 = requests.get(url1).status_code
        if r1 == 200:                                                                               # trovato nuovo con risultati
            row['link_risultati'] = url1
            row['sigma'] = 'Nuovo'
            row['status'] = 'ok'
            return row
        
        url1_1 = 'https://www.fidal.it/risultati/'+meet_year+'/' + cod + '/Iscrizioni/IndexPerGara.html'     # trovato nuovo ma senza risultati
        r1_1 = requests.get(url1_1).status_code
        if r1_1 == 200:
            row['link_risultati'] = ''
            row['sigma'] = 'Nuovo'
            row['status'] = 'link_risultati non ancora disponibili'
            return row
        
        ## Vediamo se è sigma vecchio
        url2 = 'https://www.fidal.it/risultati/'+meet_year+'/' + cod + '/RESULTSBYEVENT1.htm'                # trovato vecchio con risultati
        r2 = requests.get(url2).status_code
        if r2 == 200:
            row['link_risultati'] = url2
            row['sigma'] = 'Vecchio #1'
            row['status'] = 'ok'
            
            # Possono esistere anche /RESULTSBYEVENT2.htm, /RESULTSBYEVENT3.htm, ..., /RESULTSBYEVENTN.htm
            for jj in range(2, 30):
                
                url2_jj = 'https://www.fidal.it/risultati/'+meet_year+'/' + cod + '/RESULTSBYEVENT'+str(jj)+'.htm'
                r2_jj = requests.get(url2_jj).status_code
                if r2_jj == 200:
                    if jj == 4: print('Attenzione questa gara ha più di 3 link: '+url2_jj)
                    if jj == 21: print('ATTENZIONE questa gara ha più di 20 link: '+url2_jj)
                    row['sigma'] = 'Vecchio #'+str(jj)

                else: continue
                
            return row
        
        url2_1 = 'https://www.fidal.it/risultati/'+meet_year+'/' + cod + '/entrylistbyevent1.htm'            # trovato vecchio senza risultati
        r2_1 = requests.get(url2_1).status_code
        if r2_1 == 200:
            row['link_risultati'] = 'Non ancora disponibili'
            row['sigma'] = 'Vecchio'
            row['status'] = 'link_risultati non ancora disponibili'
            return row

        ## Se non è pan è polenta. Questo deve essere sigma vecchissimo
        row['link_risultati'] = url3
        row['sigma'] = 'Vecchissimo'
        row['status'] = 'ok'
        return row
        
    else:
        print('la risposta della pagina è ' + str(r3) + '... e mo\'?')
        return None


def get_meet_info(conn, update_criteria, where_clause=""):
    """
    Trova i link da mettere nelle colonne 'link_sigma', 'link_risultati' per
    ogni meeting aggiorna anche le colonne 'sigma', 'status', 'aggiornato' 
    perché sono utili.
    
    Inizia controllando se esiste
    'https://www.fidal.it/risultati/2024/' + cod + '/Index.htm'
    e, in caso affermativo, continua cercando la pagina di risultati e la
    versione di sigma utilizzata
    
    update_criteria: 'all' per ricontrollare tutte le righe
                     'date_N' per aggiornare solo le gare che non sono state
                              aggiornate nell'intorno di N giorni dalla data
                              della gara stessa
                     'status' aggiorna le gare che hanno status diverso da 'ok'
                              assieme al quelle che hanno Sigma Vecchio #1 e #2
                     'null'   aggiorna le righe con status null
                     'custom' utilizza la where_clause in input
    """
    
    todayis = datetime.today().date()

    # Build WHERE clause
    if update_criteria.startswith('date_'):
        # Rows I want to check: if today is 7 days from/prior to the meet
        #                       or if it wasn't updated N days after the meet
        time_span = int(update_criteria.split('_')[1])  # days around the meet
        print(f"Aggiorno i link nell'intorno di {time_span} giorni.")

        where_clause = f"""
            WHERE 
                ABS(DATE '{todayis}' - data) < {time_span}
            OR 
                (
                    ((aggiornato - data) < {time_span})
                    AND (DATE '{todayis}' - data) > 0
                )
            """

    elif update_criteria == 'status':
        print("Aggiorno i link per le gare con status diverso da 'ok' "
              "e quelle con il Sigma Vecchio #1 e #2.")
        where_clause = f"""
            WHERE
                status != 'ok'
                OR status IS NULL
                OR sigma = 'Vecchio #1'
                OR sigma = 'Vecchio #2'
        """

    elif update_criteria == 'null':
        print("Aggiorno i link per le gare con status null")
        where_clause = "WHERE status is null"
    
    elif update_criteria == 'custom':
        print(f"Uso:")
        print(where_clause)

    else:
        print("Update criteria non valido. Quelli validi sono:\n'date_N', 'status' and 'all'")
        return 
    
    query = f"SELECT * FROM gare {where_clause}"
    df_gare = pd.read_sql(query, conn)

    if df_gare.empty:
        print("Non c'è nulla da aggiornare")
        return

    ## Aggiornamento
    tot = len(df_gare)
    print(f"Aggiorno {tot} righe")

    kk = 1
    ee = 0
    for _, row in df_gare.iterrows():
        print('\t' + str(kk) + '/' + str(tot), end="\r")
        kk += 1
        row_updated = classifica_sigma(row)
        if row_updated is not None:
            row_updated['aggiornato'] = todayis
            if (row != row_updated).any():
                ee += 1
            updates_DB_gara_row(row_updated, conn)

    print(f"{ee} righe sono state aggiornate")


def get_events_link(df_gare, update_criteria, *arg):
    ## Cerca i risultati di ogni gara presente nella pagina di risultati del meeting
     # esesmpio pagina del meeting https://www.fidal.it/risultati/2024/REG33841/link_risultati/Indexlink_risultatiPerGara.html
     # Salva anche il nome con cui compare quella disciplina.
     #
     # get_events_link(DataFrame, str, DataFrame) --> DataFrame
     # 1° DataFrame in input con columns=['data','codice','home','risultati','versione sigma','status','ultimo aggiornamento']
     # 2° DataFrame in input è lo stesso di quello in output
     # DataFrame in output columns=['codice','versione sigma','warining','disciplina','nome','link']
     # 'data' and 'aggiornato' devono essere riconosciuti come date() da python
     # update_criteria: 'ok' per cotrollare tutte le gare con status 'ok'
     #                  'date_N' per controllare solo le gare svolte da N giorni. In questo caso bisogna anche dare in input il DataFrame da aggiornare
     #                           dopo update_criteria
    

    df_risultati = pd.DataFrame(columns=['codice', 'sigma', 'Warning', 'Disciplina', 'nome', 'Link'])

    ## Uniformiamo il formato delle date
    todayis = datetime.today()#date of today
    df_gare['aggiornato'] = pd.to_datetime(df_gare['aggiornato'])
    df_gare['data'] = pd.to_datetime(df_gare['data'])
    
    if update_criteria == 'ok':
        print('Aggiorno tutti i link con status = \'ok\'')
        cond_update = (df_gare['status'] == 'ok')
        
    elif update_criteria.startswith('date_'):
        time_span = int(update_criteria.split('_')[1]) # quanti giorni dopo la gara continuo a cercare risultati
        print('Aggiorno tutti i link delle gare finite da al massimo ' + str(time_span) + ' giorni')
        cond_update = (df_gare['status'] == 'ok') & ((todayis - df_gare['data']).dt.days.between(0, time_span))
        df_risultati_old = arg[0]
        
    else:
        print('Update criteria \'' + update_criteria + '\' not valid. Valids one are \'ok\' and \'date_N\' where N is an integer')
        return
    

    data = None
    updated_cods = []
    
    ## Link al sigma NUOVO
    df_links_nuovi = df_gare[(df_gare['sigma'] == 'Nuovo') & cond_update].reset_index(drop=True)
    tot = str(len(df_links_nuovi))

    if int(tot) == 0:
        print('Non ci sono link al sigma nuovo da aggiornare')

    else:
        print('\nAnalizzo i link al sigma nuovo:\n')

        for ii, row in df_links_nuovi.iterrows():
            print('\t' + str(ii+1) + '/' + tot, end="\r")
            cod = row['codice']
            updated_cods.append(cod)
            url = row['link_risultati']

            r = requests.get(url).text
            soup = BeautifulSoup(r, 'html.parser')
            elements = soup.find_all('a')
            
            for element in elements:
                link = element['href']
                
                if link[0] != '#':
                    nome = element.text.strip()
                    link = url[:-26] + link
                    data = pd.DataFrame([{'codice':cod, 'sigma':'Nuovo', 'nome':nome, 'Link':link}])
                    df_risultati = pd.concat([df_risultati, data])
            
            if data is None: print('Link vuoto: '+url)

            data = None

    ## Link al sigma VECCHIO 
    df_vecchio = df_gare[df_gare['sigma'].str.contains('#') & cond_update]
    urls = []
    for ii, row in df_vecchio.iterrows():
        cod = row['codice']
        link = row['link_risultati']

        # Con il sigma vecchio posso avere N pagine di risultati. La cella si chiama quindi 'Sigma Vecchio #N'
        N = int(row['sigma'].split('#')[1])
        # Creo un link per ognuna di queste pagine
        for jj in range(1, N+1):
                link_jj = link[:-5]+str(jj)+link[-4:]
                urls.append([cod, link_jj])

    tot = str(len(urls))
    if int(tot) == 0:
        print('Non ci sono link al sigma vecchio da aggiornare')

    else:
        print('\nAnalizzo i link al sigma vecchio:\n')

        for ii, row in enumerate(urls):
            cod = row[0]
            url = row[1]

            updated_cods.append(cod)

            print('\t' + str(ii+1) + '/' + tot, end="\r")

            r = requests.get(url).text
            soup = BeautifulSoup(r, 'html.parser')
            elements = soup.find_all('td', id='idx_colonna1')

            for element in elements:
                a_tag = element.find('a')
                if a_tag:
                    link = a_tag['href']
                    link = url[:url.rfind('/')] + '/' + link
                    nome = a_tag.get_text(strip=True)
                    data = pd.DataFrame([{'codice':cod, 'sigma':'Vecchio', 'nome':nome, 'Link':link}])
                    df_risultati = pd.concat([df_risultati, data])
            
            if data is None: print('Link vuoto: '+url)
            data = None

    ## Link al sigma VECCHISSIMO
    df_links_vecchissimi = df_gare[(df_gare['sigma'] == 'Vecchissimo') & cond_update].reset_index(drop=True)
    
    tot = str(len(df_links_vecchissimi))
    if int(tot) == 0:
        print('Non ci sono link al sigma vecchissimo da aggiornare')

    else:
        print('\nAnalizzo i link al sigma vecchissimo:\n')
    
        for ii, row in df_links_vecchissimi.iterrows():
            print('\t' + str(ii+1) + '/' + tot, end="\r")
            cod = row['codice']
            url = row['link_risultati']

            updated_cods.append(cod)

            r = requests.get(url).text
            soup = BeautifulSoup(r, 'html.parser')
            elements = soup.find_all('a') # class_='idx_link' non dovrebbe servire
            
            for element in elements:
                link = element['href']
                if link[4] != 'L':      # gli iscritti hanno il link con la L prima del numero
                    link = url[:-9] + link
                    nome = element.text.strip()
                    data = pd.DataFrame([{'codice':cod, 'sigma':'Vecchissimo', 'nome':nome, 'Link':link}])
                    df_risultati = pd.concat([df_risultati, data])
            
            if data is None: print('Link vuoto: '+url)
            data = None
                

    df_risultati['Disciplina'] = 'boh'
    df_risultati['Warning'] = ''

    if update_criteria == 'ok':
        print('Ho aggiornato ' + str(len(updated_cods)) + ' codici gare')  

    elif update_criteria.startswith('date_'):
        df_risultati_not_so_old = df_risultati_old[~df_risultati_old['codice'].isin(updated_cods)]
        len1 = str( len(df_risultati_old)-len(df_risultati_not_so_old) )
        len2 = str(len(df_risultati.reset_index(drop=True)))
        print('\nElimino '+len1+' dei risultati vecchi e ne aggiungo '+len2+' più aggiornati (forse).')

        df_risultati = pd.concat([df_risultati_not_so_old, df_risultati])

    df_risultati = df_risultati.reset_index(drop=True)
    return df_risultati


def hard_strip(nome_str):
    ## prende il nome dato a una gara a un meeting di ateltica
     # e fa del suo meglio per togliere tutte le cose inutili
     # cercando di lasciare solo le informazioni importanti
     #
     # hard_strip(str) --> str


    nome_str = nome_str.strip().lower()
    
    starting_trash = ['modello 1','modello','risultati','classifica']
    for word in starting_trash:
        if nome_str.startswith(word): return 'altro'
    
    nome_str = nome_str.replace('(', ' ').replace(')', ' ').replace('-', ' ')
    
    first_trash = ['finale','extra','ad invito','invito','piani','staffetta','staff.',' u14 ',' u15 ',' u16 ',' u17 ',' u18 ',' u19 ',' u19 ',' u20 ',' u23 ']
    for word in first_trash:
        nome_str = nome_str.replace(word, '')
    
    nome_str = nome_str.replace('metri','m').replace('ostacoli','hs')
    
    second_trash = ['salto con l\'','salto in','salto']
    for word in second_trash:
        if nome_str.startswith(word):
            nome_str = nome_str.replace(word, '')
    
    third_trash = [' ','/','bancari','indoor']
    for word in third_trash:
        nome_str = nome_str.replace(word, '')
        
    fourth_trash = ['asta','lungodafermo','lungo','alto','triplo','quadruplo']
    for word in fourth_trash:
        if nome_str.startswith(word):
            nome_str = word
    
    fifth_trash = ['pesokg1','pesokg2','pesokg3','pesokg4','pesokg5','pesokg6','pesokg7.260','pesospkg1','pesospkg2','pesospkg3','pesospkg4','pesospkg5','pesospkg6','pesospkg7.260']
    for word in fifth_trash:
        if nome_str.startswith(word):
            nome_str = word
            
    return nome_str
