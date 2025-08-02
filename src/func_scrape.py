import requests
import pandas as pd
import re
from bs4 import BeautifulSoup
from datetime import datetime
from func_general import DOMAIN
from sqlalchemy import text
from io import StringIO


def iscritti_staffetta_sigma_nuovo(iscritti, url):

    tot = int(len(iscritti) / 3)
    df = pd.DataFrame(index=range(tot),
        columns=['bib', 'atleta', 'anno', 'categoria', 'club', 'SB', 'PB', 'link_atleta'])

    j = 0
    for i, row in enumerate(iscritti):
        if i % 3 == 0:
            tds = row.find_all('td')
            bib = tds[0].text.strip()
            nome = tds[1].text.strip()
            categoria = tds[2].text.strip()
            club = tds[3].text.strip()
            SB = tds[4].text.strip()
            if len(tds) > 5:
                PB = tds[5].text.strip()
        if (i - 2) % 3 == 0:
            for a in row.find_all('a'):
                df.loc[j, 'bib'] = bib
                df.loc[j, 'categoria'] = categoria
                df.loc[j, 'club'] = club
                df.loc[j, 'SB'] = SB
                if len(tds) > 5:
                    df.loc[j, 'PB'] = PB
                df.loc[j, 'atleta'] = a.text.strip()
                df.loc[j, 'link_atleta'] = a.get("href")

                j += 1
    
    return df


def iscritti_sigma_nuovo(anno, codice, gara) -> pd.DataFrame | None:
    """
    Estrae i dati degli iscritti da una pagina SIGMA e salva nel database solo
    i nuovi atleti.

    url: URL della pagina SIGMA con la tabella iscritti
    conn: Connessione al database data da fun_general.get_db_engine()
    """
    # Richiesta
    url = f"{DOMAIN}{anno}/{codice}/Iscrizioni/{gara}"
    r = requests.get(url)
    if r.status_code != 200:
        print("\nPagina non esistente", url)
        return None 

    # Trova la tabella
    soup = BeautifulSoup(r.text, "html.parser")
    tables = soup.find_all('table', {'class': 'table table-striped table-sm table-bordered h6-7'})
    if len(tables) != 1:
        print(f"\nHo {len(tables)} tabelle: {url}")
        return None

    table = tables[0]
    rows = table.find_all('tr')
    if len(rows) < 3:
        print("\nNon ci sono iscritti", url)
        return None

    # Dati sugli iscritti
    iscritti = rows[1:-1]
    tot = len(iscritti)
    tot_check = int(rows[-1].find('td').text.strip().split(':')[-1].strip())

    if tot == 3 * tot_check: # cose strane, le staffette hanno righe in più
        df = iscritti_staffetta_sigma_nuovo(iscritti, url)

    elif tot != tot_check:
        print("\nERROR: Numero iscritti non confermato:"
              f"df={tot} vs tot={tot_check}")
        print(" "*8, url)
        return None

    else:
        df = pd.DataFrame(index=range(tot),
            columns=['bib', 'atleta', 'anno', 'categoria', 'club', 'SB', 'PB', 'link_atleta'])

        for i, tr in enumerate(iscritti):
            for j, td in enumerate(tr.find_all('td')):
                df.iloc[i, j] = td.text.strip()
                if j == 1 and td.find('a'):
                    df.iloc[i, 7] = td.find('a').get("href")
    
    return df


def iscritti_sigma_vecchio(anno, codice, gara, sigma) -> pd.DataFrame | None:

    # Richiesta
    url = f"{DOMAIN}{anno}/{codice}/{gara}"
    r = requests.get(url)
    if r.status_code != 200:
        print("\nPagina non esistente", url)
        return None 

    # Trova la tabella giusta in base al sigma
    soup = BeautifulSoup(r.text, "html.parser")
    tables = soup.find_all('table')

    if sigma == 'vecchio':
        if len(tables) < 8:
            print(f"\nHo {len(tables)} tabelle: {url}")
            return None
        table = tables[7]
    elif sigma == 'vecchissimo':
        if len(tables) < 6:
            print(f"\nHo {len(tables)} tabelle: {url}")
            return None
        table = tables[5]
    else:
        print("sigma è vecchio o vecchissimo. sigma =", sigma)
        return None

    rows = table.find_all('tr')
    if len(rows) < 3:
        print("\nNon ci sono iscritti", url)
        return None

    # Dati sugli iscritti
    iscritti = rows[1:-2]
    tot_check = int(rows[-1].find('td').text.strip().split(':')[-1].strip())

    df = pd.DataFrame(index=range(2*tot_check),
        columns=['bib', 'atleta', 'anno', 'categoria', 'club', 'SB'])

    for i, tr in enumerate(iscritti):
        for j, td in enumerate(tr.find_all('td')):
            df.iloc[i, j] = td.text.strip()

    df = df[(df['atleta'] != '') & (~df['atleta'].isna())]

    # Controlla se è una staffetta
    tot = len(df)
    if tot == 2 * tot_check or gara.startswith('Staff'): 
        df_staff = pd.DataFrame(index=range(10*tot_check),
                 columns=['bib', 'atleta', 'anno', 'categoria', 'club', 'SB'])
        jj = 0
        for ii, row in df.iterrows():
            if ii % 2 == 0:
                bib = row['bib']
                club = row['atleta']
            else:
                if '-' not in row['atleta']:
                    continue
                for name in row['atleta'].split('-'):
                    atleta = name.strip()[:-4]
                    anno = name.strip()[-4:]
                    df_staff.loc[jj, 'bib'] = bib
                    df_staff.loc[jj, 'atleta'] = atleta
                    df_staff.loc[jj, 'anno'] = anno
                    df_staff.loc[jj, 'club'] = club

                    jj += 1
        
        if df_staff.empty:
            print("Staffetta senza atleti:", url)
            return None
        df = df_staff
        tot /= 2

    # Controlla se ci sono stati altri problemi
    if tot != tot_check:
        print("\nERROR: Numero iscritti non confermato:"
              f"df={tot} vs tot={tot_check}")
        print(" "*8, url)
        return None

    return df


def iscritti_per_evento(anno, codice, gara, sigma, conn):

    if sigma == 'nuovo':
        df = iscritti_sigma_nuovo(anno, codice, gara)
        url = f"{DOMAIN}{anno}/{codice}/Iscrizioni/{gara}"

    elif sigma in ('vecchio', 'vecchissimo'):
        df = iscritti_sigma_vecchio(anno, codice, gara, sigma)
        url = f"{DOMAIN}{anno}/{codice}/{gara}"

    else:
        print("sigma è nuovo, vecchio o vecchissimo. sigma =", sigma)
        return 0
        
    if df is None:
        return 0

    # Scrivi sul database
    try:
        df['codice'] = codice
        df['gara'] = gara
        query1 = text(f"""SELECT * FROM iscritti
                      WHERE codice = :codice
                      AND gara = :gara
                      """)
        df_old = pd.read_sql(query1, conn, params={"codice": codice, "gara": gara}) 

        df_new = df[~df['atleta'].isin(df_old['atleta'])]
        df_new.to_sql('iscritti', conn, if_exists='append', index=False)

        query2 = text(f"""
                          UPDATE pagine_gara
                          SET scraped_iscr = CURRENT_TIMESTAMP
                          WHERE codice = '{codice}'
                          AND gara = '{gara}'
                       """)
        conn.execute(query2)
        conn.commit()

        conn.commit()
    except Exception as e:
        print(f"\nError: {e}")
        print(f"URL: {url}")
        print(df)
        exit()

    return len(df_new)


def get_iscritti(conn, update_condition, where_clause=''):

    where_clause = """WHERE EXTRACT(YEAR FROM data_inizio) = 2025
                      AND status IS NOT NULL
                      """
    query_cod = f"SELECT codice FROM gare {where_clause}"
    df_cod = pd.read_sql(query_cod, conn).reset_index(drop=True)

    added = 0
    tot = len(df_cod)
    ii = 0
    for cod in df_cod['codice']:
        print(f"\t{ii:d}/{tot:d}", end="\r"); ii += 1
        query_gara = text(f"""
                        SELECT anno, gara, sigma FROM pagine_gara
                        WHERE codice = '{cod}'
                        AND (gara LIKE 'GaraL%' OR gara LIKE 'Staff%')
                        AND scraped_iscr IS NULL
                     """)
        df_gara = pd.read_sql(query_gara, conn)
        if df_gara.empty: continue
        added += df_gara.apply(
            lambda row: iscritti_per_evento(row['anno'], cod, row['gara'],
                                            row['sigma'], conn), 
            axis=1
        ).sum()


def luogo_data_batteria(date_str):
    ## l'input è del tipo 'PHOTOFINISHPalaCasali Ancona - 4 gen 2024 - 11:51'
    ## Gestisce anche cose del tipo 'Raul Guidobaldi - Indoor - 13 gen 2024 - 12:52'
    ## rimuove 'PHOTOFINISH' se c'è
    ## restituisce luogo della batteria e data-ora in formato 'YYYY-MM-DD hh:mm:ss'
    
    match_data = re.search(r'\b\d+ \w+ \d{4}\b', date_str) # 17 gen 2024
    match_ora = re.findall(r'\b\d{2}:\d{2}', date_str)      # 13:57
    
    # Il luogo sembra sempre essere prima della data
    if match_data:
        data_batteria = match_data[0]
        luogo_batteria = date_str.split(data_batteria)[0].replace('PHOTOFINISH','').replace('-','').strip()
    
        mese_batteria = data_batteria.split()[1].lower().strip()[0:3]
        
        mesi = {
        'gen': 1, 'feb': 2, 'mar': 3, 'apr': 4,
        'mag': 5, 'giu': 6, 'lug': 7, 'ago': 8,
        'set': 9, 'ott': 10, 'nov': 11, 'dic': 12
        }
    
        parti = data_batteria.split()

        giorno = int(parti[0])
        mese = mesi[mese_batteria]
        anno = int(parti[2])
    
        # A volte manca l'orario, quindi se non c'è mette 00:00:00
        if match_ora:
            ora_batteria = match_ora[-1] # nel sigma vecchio spesso vengono messi l'orario di inizio e poi quello di fine.
            ora, minuti = map(int, ora_batteria.split(':'))
            data_ora = datetime(anno, mese, giorno, ora, minuti)
            
        else:
            data_ora = datetime(anno, mese, giorno)
            
        return luogo_batteria, data_ora
    
    
    # Se non c'è il giorno
    else: return date_str, ''
    

def clean_tempo(tempo):
    
    tempo = str(tempo)
    tempo = tempo.upper()
    
    if 'DNF' in tempo: return 'DNF'
    if 'DNS' in tempo: return 'DNS'
    if 'NM' in tempo: return 'NM'
    if 'DQ' in tempo: return 'DSQ'
    
    IQ_match = re.match(r'(\d[\d.:]*\d)', str(tempo))
    if IQ_match: return IQ_match[0]
    else: return 'boh'


def clean_nome(nome):
    
    nome = nome.replace('(I)','')
    nome = nome.replace('(FC)','')
    nome = nome.replace('Campionessa Italiana','')
    nome = nome.replace('Campione Italiano','').strip()
    
    return nome


def scrape_nuovo_corse(comptetition_row):
    ## Funzione per fare scraping dei risultati delle corse (individuali), degli ostacoli e della marcia nel sito nuovo ('Versione Sigma' = 'Nuovo')
    ## input è una riga di un DataFrame con columns=['Codice','Versione Sigma','Warning','Disciplina','Nome','Link']
    ## Se la versione del sigma in input non è corretta la funzione stampa un errore e non restituisce nulla
    ## Se la disciplina non è corretta stampa un errore e restituisce un DataFrame vuoto
    ## Restituisce una DataFrame con columns=['Disciplina', 'Prestazione', 'Atleta','Anno','Categoria','Società','Data','Luogo','Gara]
    
    # Ogni tabella è preceduta da una <div class='row' con alcune informazioni(data, luodo, numero di batteria/finale/serie o se è un riepilogo)
    # devo ancora gestire le start list
    
    # Controllo la versione del sigma
    if comptetition_row['Versione Sigma'] != 'Nuovo':
        print('Versione sigma '+comptetition_row['Versione Sigma']+'. Questa funzione funziona con il sigma nuovo')
        return
    
    url = comptetition_row['Link']
    disciplina = comptetition_row['Disciplina']
    batterie = pd.DataFrame(columns=['Disciplina', 'Prestazione', 'Atleta','Anno','Categoria','Società','Data','Luogo','Gara'])
    
    # Controllo che sia una corsa individuale o la marcia
    if not(disciplina[0].isdigit() or disciplina.startswith('Marcia')) or 'x' in disciplina:
        print('Non compatibile con '+disciplina+'. Solo corse individuali e marcia.')
        return batterie
    
    # Recupero le linee di testo della pagina (titoli nomi delle batterie per la maggior parte)
    r = requests.get(url).text
    soup = BeautifulSoup(r, 'html.parser')
    div_righe = soup.find_all('div', class_='row')

    div_batterie = []
    for a in div_righe[2:]: # le prime 2 div sono di intestazione
        if not(('risultati' in a.text.lower()) or ('results' in a.text.lower()) ): # anche le div con scritto risultati/results sono inutili
            div_batterie.append(a)


    # Ora prendo tutte le tabelle che ci sono
    dfs = pd.read_html(r)
    if dfs[0].iloc[0,0] == 'Record': dfs = dfs[1:]  # a volte la prima tabella ha i record della disciplina
    
    if len(dfs) != len(div_batterie): # controllo se ho filtrato correttamente tabelle e titolo delle tabelle
        print(url)
        print('Ho trovato ' + str(len(div_batterie)) + ' div e ' + str(len(dfs)) + ' tabelle:')
        
    else:
        for a,df in zip(div_batterie, dfs):
            if not(('riepilogo' in a.text.lower()) or ('summary' in a.text.lower())): # la tabella con il riepilogo contiene gli stessi risultati delle altre (se c'è)
                
                if 'Atleta' in df: colonna_atleta = df.columns.get_loc('Atleta')
                elif 'Athlete' in df: colonna_atleta = df.columns.get_loc('Athlete')
                else: print('Non trovo la colonna atleta: ' + url)
                
                batteria_N = df.iloc[:, colonna_atleta:colonna_atleta+5]
                
                while batteria_N.iloc[-1,0] == batteria_N.iloc[-1,1]:   # le ultime righe hanno cose che non sono risultati. Vanno tolte e
                    batteria_N = batteria_N.iloc[:-1,:]                 # sfrutto il fatto che sono la stessa cella ripetutta per tutta la riga
                
                (luogo_batteria, data_batteria) = luogo_data_batteria(a.find_all('p')[-1].text)
                
                batteria_N['Data'] = data_batteria
                batteria_N['Luogo'] = luogo_batteria
                batteria_N['Disciplina'] = disciplina
                
                batteria_N = batteria_N.iloc[:, [7, 4, 0, 1, 2, 3, 5, 6]]
                batteria_N.columns = ['Disciplina', 'Prestazione', 'Atleta','Anno','Categoria','Società','Data','Luogo']
                batteria_N['Gara'] = url
                batteria_N['Prestazione'] = batteria_N['Prestazione'].apply(clean_tempo)
                batteria_N['Atleta'] = batteria_N['Atleta'].apply(clean_nome)

                batterie = pd.concat([batterie, batteria_N]).reset_index(drop=True)
    
    return batterie
            

def scrape_vecchio_corse(competition_row):
    ## Funzione per fare scraping dei risultati delle corse (individuali), degli ostacoli e della marcia nel sito nuovo ('Versione Sigma' = 'Nuovo')
    ## input è una riga di un DataFrame con columns=['Codice','Versione Sigma','Warning','Disciplina','Nome','Link']
    ## Se la versione del sigma in input non è corretta la funzione stampa un errore e non restituisce nulla
    ## Se la disciplina non è corretta stampa un errore e restituisce un DataFrame vuoto
    ## Restituisce una DataFrame con columns=['Disciplina', 'Prestazione', 'Atleta','Anno','Categoria','Società','Data','Luogo','Gara]
    
    # Controllo la versione del sigma
    if competition_row['Versione Sigma'] != 'Vecchio':
        print('Versione sigma '+competition_row['Versione Sigma']+'. Questa funzione funziona con il sigma nuovo')
        return
    
    url = competition_row['Link']
    disciplina = competition_row['Disciplina']
    batterie = pd.DataFrame(columns=['Disciplina', 'Prestazione', 'Atleta','Anno','Categoria','Società','Data','Luogo','Gara'])
    
    # Controllo che sia una corsa individuale o la marcia
    if not(disciplina[0].isdigit() or disciplina.startswith('Marcia')) or 'x' in disciplina:
        print('Non compatibile con '+disciplina+'. Solo corse individuali e marcia.')
        return batterie
    
    r = requests.get(url).text
    soup = BeautifulSoup(r, 'html.parser')

    # Ora posso cominciare a scaricare le tabelle della pagina
    
    soup_tabelle = soup.find_all('table')

    dfs_risultati = []
    if soup_tabelle:
        
        for soup_tab in soup_tabelle:
            
            # Righe delle tabelle
            rows = soup_tab.find_all('tr', class_=lambda x: x in ['due', 'uno'])
            
            tabella_N = []  # tabella N-esima
            for row in rows:
                
                cells = row.find_all('td') # celle della riga
                
                for i, cell in enumerate(cells):
                    if cell.get('id') == 't1_atle': # DEVE esistere almeno la cella Atleta
                        
                        # Assumendo che ci sia sempre Atleta, Anno, Categoria, Società, Prestazione
                        atle_text = cell.text.strip()
                        anno_text = cells[i+1].text.strip() if i+1 < len(cells) else ''
                        cate_text = cells[i+2].text.strip() if i+2 < len(cells) else ''
                        soci_text = cells[i+3].text.strip() if i+3 < len(cells) else ''
                        pres_text = cells[i+4].text.strip() if i+4 < len(cells) else ''
                        
                        data = [atle_text, anno_text, cate_text, soci_text, pres_text]
                        
                        # Se tabella_N è non vuota, controllo che l'ultima riga non sia uguale a quella che voglio aggiungere
                        if tabella_N:
                            if tabella_N[-1] != data: tabella_N.append(data)
                        else: tabella_N.append(data)
                        
                        # devo interrompere perché causa <tr> non chiusi nel codice html potrei continuare il ciclo sulle celle della riga dopo
                        break
                    
            if tabella_N:
                df_N = pd.DataFrame(tabella_N, columns=['Atleta','Anno','Categoria','Società','Prestazione'])
                dfs_risultati.append(df_N)
        
    else: print('La pagina è senza tabelle '+url)
    
    # la prima tabella è una tabella con tutte le righe delle altre tabelle. Credo succeda per qualche errore di sitassi nel file html
    dfs_risultati = dfs_risultati[1:]
    
    # Ora prendo i titoli delle batterie assieme alla riga dove c'è scritto data e ora
    
    titoli = soup.find_all('td', class_='tab_turno_titolo')
    dataora_tutti = soup.find_all('td', class_='tab_turno_dataora')
    
    # Se il titolo è 'riepilogo', allora quella dataora e quella tabella non mi interessano. In questo modo dovrei rimanere solo con batterie/serie/finali
    
    if len(titoli) != len(dfs_risultati): # controllo se ho filtrato correttamente tabelle e titolo delle tabelle
        print(url)
        print('Ho trovato ' + str(len(titoli)) + ' titoli e ' + str(len(dfs_risultati)) + ' tabelle:')
        
    else:
        for titolo, a, df in zip(titoli, dataora_tutti, dfs_risultati):
            if not('riepilogo' in titolo.text.lower()):
                
                batteria_N = df[df['Atleta'] != df['Prestazione']].copy()
                
                (luogo_batteria, data_batteria) = luogo_data_batteria(a.text)
                
                batteria_N['Data'] = data_batteria
                batteria_N['Luogo'] = luogo_batteria
                batteria_N['Disciplina'] = disciplina
                batteria_N['Gara'] = url
                
                batteria_N['Prestazione'] = batteria_N['Prestazione'].apply(clean_tempo)
                batteria_N['Atleta'] = batteria_N['Atleta'].apply(clean_nome)

                batteria_N = batteria_N[['Disciplina','Prestazione','Atleta','Anno','Categoria','Società','Data','Luogo','Gara']]

                batterie = pd.concat([batterie, batteria_N]).reset_index(drop=True)
                
    return batterie
