import re
from datetime import date, datetime

import pandas as pd
import requests
from bs4 import BeautifulSoup
from sqlalchemy import MetaData, Table, create_engine, literal_column, or_, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.engine import Connection

from config import DB_CONFIG

DOMAIN = "https://www.fidal.it/risultati/"

# Se a CHECK404 giorni di distanza dalla fine della gara un link da ancora 404
# lo segno come scraped e basta
CHECK404 = 14


def get_sqlalchemy_connection_string():
    """Generates the connection string for SQLAlchemy."""
    return f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"


################################################################################
#           Scaricare il calendario FIDAL con la lista delle gare              #
################################################################################


def get_db_engine():
    """Create and return SQLAlchemy engine."""
    connection_string = get_sqlalchemy_connection_string()
    return create_engine(connection_string)


def extract_meet_codes_from_calendar(
    anno, mese, livello, regione, tipo, categoria
) -> pd.DataFrame:
    """
    Scarica informazioni sulle gare presenti nel calendario fidal https://www.fidal.it/calendario.php
    'aggiornato' è messo di defaul al 31 marzo 1896, data simbolica per dire che i risultati
    non sono stati aggiornati di recente

    Input: fare riferimento a Readme.txt
    Output: df['data_inizio' 'data_fine' 'codice' 'aggiornato' 'nome'
               'link_gara' 'livello' 'luogo' 'tipologia']
    """

    ## Componiamo il link con i parametri del filtro
    url = (
        f"https://www.fidal.it/calendario.php?"
        f"anno={anno}&"
        f"mese={mese}&"
        f"livello={livello}&"
        f"new_regione={regione}&"
        f"new_tipo={tipo}&"
        f"new_categoria={categoria}&"
        f"submit=Invia"
    )

    response = requests.get(url)

    if response.status_code != 200:
        print("Failed to fetch the webpage. status code:", response.status_code)
        return pd.DataFrame()

    soup = BeautifulSoup(response.text, "html.parser")
    table = soup.find("table", class_="table")

    data_inizio_gara = []
    data_fine_gara = []
    cod_gara = []
    nome_gara = []
    link_gara = []
    luogo_gara = []
    livello_gara = []
    tipologia_gara = []

    if not table:
        print("Non ho trovato nulla")
        return pd.DataFrame()

    ## Testo con la data del meeting
    for row in table.find_all("tr"):
        tds = row.find_all("td")
        if len(tds) != 6:
            print("Riga strana: " + url)
            continue

        meet_date = tds[1].get_text(strip=True)
        last_day = int(meet_date[-5:-3])
        last_month = int(meet_date[-2:])
        data_fine_gara.append(date(int(anno), last_month, last_day))
        first_day = int(meet_date[:2])
        if meet_date[2] == "-":
            first_month = last_month
        elif meet_date[2] == "/":
            first_month = int(meet_date[3:5])
        else:
            print("data strana", meet_date, url)
            first_month = last_month
        data_inizio_gara.append(date(int(anno), first_month, first_day))

        livello_gara.append(tds[2].get_text(strip=True))

        nome_gara.append(tds[3].get_text(strip=True))
        link_gara.append(tds[3].find("a")["href"])
        cod_gara.append(link_gara[-1].split("/")[-1].strip())

        tipologia_gara.append(tds[4].get_text(strip=True).lower())

        luogo_gara.append(tds[5].get_text(strip=True))

    df = pd.DataFrame(
        {
            "data_inizio": data_inizio_gara,
            "data_fine": data_fine_gara,
            "codice": cod_gara,
            "aggiornato": date(1896, 3, 31),
            "nome": nome_gara,
            "link_gara": link_gara,  # first modern olympics date
            "livello": livello_gara,
            "luogo": luogo_gara,
            "tipologia": tipologia_gara,
        }
    )

    return df


################################################################################
#               Per ogni gara controlla la versione del sigma                  #
#                   e se ci sono gli iscritti/risultati                        #
################################################################################


def update_gare_database(anno, mese="", regione="", categoria="", tipo=""):
    """Update the gare table with new meet codes."""
    engine = get_db_engine()

    # Get new meet codes
    df_REG_gare = extract_meet_codes_from_calendar(
        anno, mese, "REG", regione, tipo, categoria
    )
    df_COD_gare = extract_meet_codes_from_calendar(
        anno, mese, "COD", regione, tipo, categoria
    )

    df_gare = pd.concat([df_REG_gare, df_COD_gare], ignore_index=True)
    if df_gare.empty:
        print(
            "df_REG_gare e df_COD_gare è 'None' per"
            f"anno {anno}, mese {mese}, regione {regione}, categoria {categoria}, tipo {tipo}"
            ", exiting..."
        )
        return

    df_gare["status"] = None
    df_gare["sigma"] = None

    # Get existing codes from database
    with engine.connect() as conn:
        query = f"SELECT codice FROM gare WHERE EXTRACT(YEAR FROM data_inizio) = {anno}"
        existing_codes = pd.read_sql(query, conn)

        # Filter new records
        new_records = df_gare[~df_gare["codice"].isin(existing_codes["codice"])]

        if not new_records.empty:
            print(f"Aggiungo {len(new_records['codice'])} nuovi codici gara")

            # Insert new records
            new_records.to_sql("gare", conn, if_exists="append", index=False)
            conn.commit()
            print("Salvataggio completato!")

        else:
            print(
                "Nessun nuovo codice gara da aggiungere per "
                f"anno {anno}, mese {mese}, regione {regione}, categoria {categoria}, tipo {tipo}"
            )


def status_value(status: str | None) -> int:
    """
    valori bassi = status migliore
    -1 dead, non aggiornare più
    -1 ghost, pagine che hanno un redirect a un'altra gara
    99 None, aggiornare sempre

    Per evitare di peggiorare lo status di una gara a causa del sito che si
    comporta in modo strano (e.g. ho già status = risultati, ma rifaccio un
    controllo su quella gara, la pagina momentaneamente non è raggiungibile
    e rischio di mettere status = 404) diamo un valore allo status.
    Questo ci consente di sapere se stiamo facendo un downgrade
    """

    if status in ["dead", "ghost"]:
        return -1
    elif status == "risultati":
        return 0
    elif status == "iscritti":
        return 1
    elif status == "vuoto":
        return 2
    elif status == "404":
        return 3
    elif status is None:
        return 99
    else:
        return 3


def updates_DB_gara_row(
    codice: str,
    sigma: str | None,
    status: str,
    link_iscritti: str | None,
    link_risultati: str | None,
    conn: Connection,
) -> None:
    """
    Modifica le colonne sigma, status, link_iscritti, link_risultati della riga
    di una gara. sigma, link_iscritti e link_risultati possono essere None, nel
    qual caso diventano NULL nel db.
    """

    sql = text("""
        UPDATE gare SET
            sigma = :sigma,
            status = :status,
            link_iscritti = :link_iscr,
            link_risultati = :link_ris,
            aggiornato = CURRENT_TIMESTAMP
        WHERE codice = :codice
    """)
    conn.execute(
        sql,
        {
            "sigma": sigma,
            "status": status,
            "link_iscr": link_iscritti,
            "link_ris": link_risultati,
            "codice": codice,
        },
    )
    conn.commit()


def _link_vecchio(first_url: str, base_url: str) -> str | None:
    """
    Per il sigma vecchio: se first_url esiste (200) restituisce il link
    all'ultima tab (o first_url se non ci sono tab), altrimenti None.
    Può sollevare requests.RequestException.
    """
    r = requests.get(first_url, timeout=15)
    if r.status_code != 200:
        return None

    soup = BeautifulSoup(r.text, "html.parser")
    links = soup.find_all("a", class_="navrinmenu")
    if links:
        return base_url + str(links[-1]["href"])
    return first_url


def classifica_sigma(
    codice: str, anno: str, status: str | None, conn: Connection, allow_downgrade=False
) -> bool:
    """
    Capisce che versione di sigma viene utilizzato a una gara (nuovo, vecchio,
    vecchissimo), aggiorna sul db la riga della gara con la versione e i link
    a iscritti e risultati (NULL se non trovati).

    Restituisce True se è stata scritta la riga del DB della gara, False se
    la home della gara non esiste o non è raggiungibile.
    """

    current_status = status_value(status)

    # Link alla home comune a tutti i sistemi
    base_url = f"{DOMAIN}{anno}/{codice}/"
    link_index = f"{base_url}Index.htm"

    try:
        response_index = requests.get(link_index, timeout=15)
    except requests.RequestException as exc:
        print(f"Errore nel collegamento a {link_index}: {exc}")
        return False

    if response_index.status_code == 404:
        status = "404"
        if status_value(status) < current_status or allow_downgrade:
            updates_DB_gara_row(codice, None, status, None, None, conn)
            return True

        return False

    if response_index.status_code != 200:
        print(f"la risposta di {link_index} è {response_index.status_code}... e mo'?")
        status = str(response_index.status_code)
        if status_value(status) < current_status or allow_downgrade:
            updates_DB_gara_row(codice, None, status, None, None, conn)
            return True

        return False

    soup_index = BeautifulSoup(response_index.text, "html.parser")
    # Versione del sigma nuova (WISE)
    if "WISE:" in response_index.text:
        # Se la pagina è un link regionale alla gara nazionale fa un redirect,
        # ma prima di farlo apre questa pagina qui
        if 'http-equiv="refresh"' in response_index.text:
            status = "ghost"
            if status_value(status) < current_status or allow_downgrade:
                updates_DB_gara_row(codice, None, status, None, None, conn)
                return True
            return False

        status = "vuoto"
        # Ottieni i link che compaiono in home, i più noti sono:
        # Iscrizioni/IndexPerGara.html
        # Iscrizioni/IndexPerCategoria.html
        # Iscrizioni/IndexPerTeam.html
        # Risultati/IndexRisultatiPerGara.html
        # Risultati/IndexRisultatiPerCategoria.html
        # Ci affidiamo al fatto che prendiamo il primo che compare con
        # 'Iscrizioni' e 'Risultati' e usiamo quello

        links = [a["href"] for a in soup_index.find_all("a", class_="link-style")]
        link_iscritti = next((str(x) for x in links if "Iscrizioni" in x), None)
        link_risultati = next((str(x) for x in links if "Risultati" in x), None)

        # NOTA: la presenza dei risultati non implica quella degli iscritti
        # (anche se nella maggioranza dei casi è così).
        if link_iscritti:
            status = "iscritti"
            link_iscritti = base_url + link_iscritti
        if link_risultati:
            status = "risultati"
            link_risultati = base_url + link_risultati

        if status_value(status) < current_status or allow_downgrade:
            updates_DB_gara_row(
                codice, "nuovo", status, link_iscritti, link_risultati, conn
            )
            return True

        return False

    ## Versione del sigma vecchio
    # Iscritti e risultati sono indipendenti: può esserci uno, l'altro o
    # entrambi. Solo se non c'è nessuno dei due passiamo al vecchissimo.
    try:
        link_iscritti = _link_vecchio(f"{base_url}ENTRYLISTBYEVENT1.htm", base_url)
        link_risultati = _link_vecchio(f"{base_url}RESULTSBYEVENT1.htm", base_url)
    except requests.RequestException as exc:
        print(f"Errore nel collegamento a {base_url}: {exc}")
        return False

    if link_iscritti or link_risultati:
        if link_risultati:
            status = "risultati"
        else:
            status = "iscritti"

        # Nota: con il vecchio mettiamo <= perché se ho già i risultati e
        # ritrovo i risultati è possibile che ci sia una tab in più
        if status_value(status) <= current_status or allow_downgrade:
            updates_DB_gara_row(
                codice, "vecchio", status, link_iscritti, link_risultati, conn
            )
            return True

        return False

    # Versione del sigma vecchissimissima (quella BLU)
    if "0000FF" in response_index.text and "Verdana" in response_index.text:
        if status_value(status) <= current_status or allow_downgrade:
            updates_DB_gara_row(codice, "vecchissimissimo", "dead", None, None, conn)
            return True
        return False

    ## Se non è pan è polenta. Questo deve essere sigma vecchissimo

    # Qui iscritti e risultati sono sempre alla stessa pagina. Quindi mettiamo
    # sempre il link agli iscritti e lo status "iscritti"
    status = "iscritti"
    if status_value(status) < current_status or allow_downgrade:
        updates_DB_gara_row(codice, "vecchissimo", status, link_index, None, conn)
        return True

    return False


def get_meet_info(
    conn: Connection, update_condition: str, where_clause="", allow_downgrade=False
):
    """
    Controlla la versione del sigma, se ci sono iscritti e/o risultati e lascia
    la data dell'ultimo controllo per ogni gara filtrata da

    update_condition:
        'date_N' per aggiornare solo le gare che non sono state aggiornate
                 nell'intorno di N giorni dalla data della gara stessa
        'status' aggiorna le gare che hanno speranza di aver ricevuto
                 aggiornamenti (status != dead, risultati) e ogni sigma vecchio
                 status != dead
        'custom' utilizza la where_clause in input
    """

    todayis = datetime.now().astimezone().date()

    # Build WHERE clause
    if update_condition.startswith("date_"):
        # Rows I want to check: if today is 7 days from/prior to the meet
        #                       or if it wasn't updated N days after the meet
        time_span = int(update_condition.split("_")[1])  # days around the meet
        print(f"Aggiorno i link nell'intorno di {time_span} giorni.")

        where_clause = f"""
            WHERE ABS(DATE '{todayis}' - data_inizio) < {time_span}
            OR ((aggiornato::date - data_fine) < {time_span}
                AND (data_fine < DATE '{todayis}'))
            """

    elif update_condition == "status":
        print(
            "Aggiorno i link per le gare con status diverso da 'dead', 'risultati' "
            "e quelle con il Sigma vecchio con status diverso da 'dead'"
        )
        where_clause = """
            WHERE
                status not in ('ghost', 'dead', 'risultati')
                OR status IS NULL
                OR (sigma = 'vecchio' and status = 'risultati')
        """

    elif update_condition == "custom":
        if where_clause == "":
            print("where_clause vuota, non aggiornerò tutto il DB.")
            return
        print("Uso:")
        print(where_clause)

    else:
        print(
            "Update criteria non valido. Quelli validi sono:\n'date_N', 'status' and 'custom'"
        )
        return

    query = f"SELECT * FROM gare {where_clause}"
    df_gare = pd.read_sql(query, conn).reset_index(drop=True)

    if df_gare.empty:
        print("Non c'è nulla da aggiornare")
        return

    ## Aggiornamento
    tot = len(df_gare)
    print(f"Aggiorno {tot} righe")

    jj = 0
    for ii, row in df_gare.iterrows():
        print(f"\t{ii:d}/{tot:d}", end="\r")

        results = classifica_sigma(
            row["codice"],
            str(row["data_inizio"].year),
            row["status"],
            conn,
            allow_downgrade,
        )

        if results:
            jj += 1

    print(f"{jj} righe sono state aggiornate")


################################################################################
#               Per ogni gara scarica tutti i link che ci sono                 #
#                           a iscritti/risultati                               #
################################################################################


def handle_dead_link(
    status_code: int, codice: str, data_fine: date, tipo: str, conn: Connection
) -> None:
    """
    Se la pagina dà 404 da più di CHECK404 giorni dopo data_fine, segna il link
    come controllato (tipo = 'iscritti' | 'risultati').
    """
    if status_code != 404:
        return

    morto_da = (datetime.now().astimezone().date() - data_fine).days
    if morto_da <= CHECK404:
        return

    if tipo not in ("iscritti", "risultati"):
        raise ValueError(f"tipo non valido: {tipo}")
    colonna = f"scraped_link_{tipo}"

    print(
        f"Sono passati più di {CHECK404} giorni, segno il link_{tipo} come controllato"
    )
    conn.execute(
        text(f"UPDATE gare SET {colonna} = CURRENT_TIMESTAMP WHERE codice = :codice"),
        {"codice": codice},
    )
    conn.commit()


def update_DB_pagine_gara(
    link_gara: str, codice: str, data: pd.DataFrame, conn: Connection
) -> int:
    """
    Upsert su pagine_gara (chiave unica: link).
    Restituisce il numero di righe nuove + quelle realmente modificate.
    """
    if data.empty:
        return 0

    data = data.drop_duplicates("link", keep="last")  # to be sure
    data = data.astype(object).where(data.notna(), None)  # NaN -> NULL
    records = data.to_dict("records")

    # Colonne da sovrascrivere se il link esiste già
    update_cols = ["nome", "tipo"]

    t = Table("pagine_gara", MetaData(), autoload_with=conn)
    stmt = insert(t).values(records)
    stmt = stmt.on_conflict_do_update(
        index_elements=["link"],
        set_={c: stmt.excluded[c] for c in update_cols},
        where=or_(*[t.c[c].is_distinct_from(stmt.excluded[c]) for c in update_cols]),
    ).returning(t.c.link, literal_column("xmax = 0").label("inserted"))

    result = conn.execute(stmt).all()
    if data["sigma"][0] == "vecchissimo":
        conn.execute(
            text("""UPDATE gare
                    SET scraped_link_iscritti = CURRENT_TIMESTAMP
                    WHERE codice = :codice"""),
            {"codice": codice},
        )
    else:
        if data["tipo"].eq("iscritti").any():
            conn.execute(
                text("""UPDATE gare
                        SET scraped_link_iscritti = CURRENT_TIMESTAMP
                        WHERE codice = :codice"""),
                {"codice": codice},
            )
        if data["tipo"].isin(["risultati", "start lists"]).any():
            conn.execute(
                text("""UPDATE gare
                        SET scraped_link_risultati = CURRENT_TIMESTAMP
                        WHERE codice = :codice"""),
                {"codice": codice},
            )

    conn.commit()

    n_new = sum(1 for _, inserted in result if inserted)
    print(f"{n_new} nuove, {len(result) - n_new} modificate \t {link_gara}")
    return len(result)


def link_sigma_nuovo(row, conn: Connection) -> int:
    """
    Usata da get_events_link(). Link di esempio
    https://www.fidal.it/risultati/2025/REG38222/Risultati/IndexRisultatiPerGara.html
    https://www.fidal.it/risultati/2026/REG43319/Iscrizioni/IndexPerCategoria.html
    """
    tipologia = row["tipologia"]
    if tipologia == "indoor":
        ambiente = "I"
    elif tipologia in ("outdoor", "pista", "piazza e altri ambiti"):
        ambiente = "P"
    else:
        print(f"Non conosco la tipologia {tipologia}")
        return 0

    records = []
    codice = row["codice"]

    # Iscritti
    found = False
    link_iscritti = row["link_iscritti"]
    if link_iscritti:
        url_iscritti = link_iscritti.rsplit("/", 1)[0]

        r = requests.get(link_iscritti, timeout=30)
        if r.status_code != 200:
            print(
                f"WARNING: {link_iscritti} non risponde, (status_code = {r.status_code})"
            )
            handle_dead_link(r.status_code, codice, row["data_fine"], "iscritti", conn)

        else:
            for el in BeautifulSoup(r.text, "html.parser").find_all(
                "a", class_="link-style"
            ):
                href = el["href"][:50]  # :50 is a safety limit
                if not href or href.startswith(("#", "http")):
                    continue
                records.append(
                    {
                        "tipo": "iscritti",
                        "link": f"{url_iscritti}/{href}",
                        "nome": el.text.strip()[:500],
                    }
                )
                found = True

            if not found:
                print(f"WARNING: Link iscritti vuoto {link_iscritti}")
                conn.execute(
                    text("""UPDATE gare
                            SET scraped_link_iscritti = CURRENT_TIMESTAMP, status = 'iscritti?'
                            WHERE codice = :codice"""),
                    {"codice": codice},
                )
                conn.commit()

    # Risultati
    found = False
    link_risultati = row["link_risultati"]
    if link_risultati:
        url_risultati = link_risultati.rsplit("/", 1)[0]

        r = requests.get(link_risultati, timeout=30)
        if r.status_code != 200:
            print(
                f"WARNING: {link_risultati} non risponde, (status_code = {r.status_code})"
            )
            handle_dead_link(r.status_code, codice, row["data_fine"], "risultati", conn)

        else:
            for el in BeautifulSoup(r.text, "html.parser").find_all(
                "a", class_="link-style"
            ):
                href = el.get("href", "")[:50]  # :50 is a safety limit
                if not href or href.startswith(("#", "http")):
                    continue

                is_risultati = el.find(
                    "img", src=lambda src: src and "checked.png" in src
                )

                records.append(
                    {
                        "tipo": "risultati" if is_risultati else "start lists",
                        "link": f"{url_risultati}/{href}",
                        "nome": el.get_text(strip=True)[:500],
                    }
                )
                found = True

            if not found:
                print(f"WARNING: Link risultati vuoto {link_risultati}")
                conn.execute(
                    text("""UPDATE gare
                            SET scraped_link_risultati = CURRENT_TIMESTAMP, status = 'risultati?'
                            WHERE codice = :codice"""),
                    {"codice": codice},
                )
                conn.commit()

    if records:
        data = pd.DataFrame(records)
        data["codice"] = codice
        data["data_inizio"] = row["data_inizio"]
        data["sigma"] = row["sigma"]
        data["ambiente"] = ambiente

        return update_DB_pagine_gara(row["link_gara"], codice, data, conn)
    else:
        print(f"WARNING: {row['link_gara']} pagina vuota")
        return 0


def link_risultati_sigma_vecchio(row, conn):
    """
    Usata da get_events_link()
    """
    tipologia = row["tipologia"]
    if tipologia == "indoor":
        ambiente = "I"
    elif tipologia in ("outdoor", "pista", "piazza e altri ambiti"):
        ambiente = "P"
    else:
        print(f"Non conosco la tipologia {tipologia}")
        return 0

    codice = row["codice"]
    # Con il sigma vecchio posso avere N pagine di risultati. link_iscritti
    # contiene l'ultima ENTRYLISTBYEVENTN.htm, poi mi basta diminuire N
    # stessa cosa per i risultati
    urls = []
    link_iscritti = row["link_iscritti"]
    if link_iscritti:
        N = int(re.search(r"\d+", link_iscritti[-9:]).group())
        for jj in range(N):
            url = link_iscritti.replace(f"{N}.htm", f"{N - jj}.htm")
            urls.append(["iscritti", url])

    link_risultati = row["link_risultati"]
    if link_risultati:
        N = int(re.search(r"\d+", link_risultati[-9:]).group())
        for jj in range(N):
            url = link_risultati.replace(f"{N}.htm", f"{N - jj}.htm")
            urls.append(["risultati", url])

    data = pd.DataFrame(columns=["tipo", "link", "nome"])
    for tipo, url in urls:
        r = requests.get(url)
        if r.status_code != 200:
            print(
                f"WARNING: {link_iscritti} non risponde, (status_code = {r.status_code})"
            )
            handle_dead_link(r.status_code, codice, row["data_fine"], tipo, conn)

        else:
            soup = BeautifulSoup(r.text, "html.parser")
            elements = soup.find_all("td", id="idx_colonna1")

            for element in elements:
                a_tag = element.find("a")
                if a_tag:
                    gara = a_tag["href"][:50]
                    if gara.startswith("http"):
                        continue
                    nome = a_tag.get_text(strip=True)[:500]
                    base_url = url.rsplit("/", 1)[0]
                    data.loc[len(data)] = [tipo, f"{base_url}/{gara}", nome]

    if link_iscritti and not data["tipo"].eq("iscritti").any():
        print(f"WARNING: Link iscritti vuoto {link_iscritti}")
        conn.execute(
            text("""UPDATE gare
                    SET scraped_link_iscritti = CURRENT_TIMESTAMP, status = 'iscritti?'
                    WHERE codice = :codice"""),
            {"codice": codice},
        )
        conn.commit()
    if link_risultati and not data["tipo"].eq("risultati").any():
        print(f"WARNING: Link risultati vuoto {link_risultati}")
        conn.execute(
            text("""UPDATE gare
                    SET scraped_link_risultati = CURRENT_TIMESTAMP, status = 'risultati?'
                    WHERE codice = :codice"""),
            {"codice": codice},
        )
        conn.commit()

    if not data.empty:
        data["codice"] = codice
        data["data_inizio"] = row["data_inizio"]
        data["sigma"] = row["sigma"]
        data["ambiente"] = ambiente

        return update_DB_pagine_gara(row["link_gara"], codice, data, conn)
    else:
        print(f"WARNING: {row['link_gara']} pagina vuota")
        return 0


def link_risultati_sigma_vecchissimo(row, conn):
    """
    Usata da get_events_link()
    Non ci perderò troppo tempo perché tanto è una pagina sola che nel 2015 è
    sempre bianca quindi lascio il codice fallire silenziosamente e amen
    """
    tipologia = row["tipologia"]
    if tipologia == "indoor":
        ambiente = "I"
    elif tipologia in ("outdoor", "pista", "piazza e altri ambiti"):
        ambiente = "P"
    else:
        print(f"Non conosco la tipologia {tipologia}")
        return 0

    codice = row["codice"]
    link = row["link_iscritti"]
    base_url = link.rsplit("/", 1)[0]

    r = requests.get(link).text
    soup = BeautifulSoup(r, "html.parser")

    righe = []

    for n in (1, 2, 3):
        # La colonna può non esistere: find_all restituisce [] e si passa oltre
        links = []
        for element in soup.find_all("td", id=f"idx_colonna{n}"):
            a_tag = element.find("a")
            if not a_tag or not a_tag.get("href"):
                continue
            href = a_tag["href"]
            if href.startswith("http"):
                continue
            links.append((href[:50], a_tag.get_text(strip=True)[:500]))

        # Il tipo dell'intera colonna si decide dal primo link riconoscibile
        tipo = None
        for href, _ in links:
            nome_file = href.split("?")[0].split("#")[0].rsplit("/", 1)[-1]
            if re.fullmatch(r"(GaraL|Staff)\d+\.htm", nome_file, re.IGNORECASE):
                tipo = "iscritti"
                break
            if re.fullmatch(r"(Gara\d+|Diffr.*)\.htm", nome_file, re.IGNORECASE):
                tipo = "risultati"
                break

        # Tipo non riconosciuto: è come se la colonna non esistesse
        if tipo is None:
            continue

        for href, nome in links:
            righe.append((tipo, f"{base_url}/{href}", nome))

    if not righe:
        # Nessuna colonna riconosciuta: lo classifico come vuoto e poi a mano
        # controllo

        print(f"WARNING: {link} vuoto")
        conn.execute(
            text("""
            UPDATE gare SET
                status = 'vuoto',
                link_iscritti = NULL,
                link_risultati = NULL
            WHERE codice = :cod
                          """),
            {"cod": codice},
        )
        conn.commit()

        return 0

    data = pd.DataFrame(righe, columns=["tipo", "link", "nome"])
    data["codice"] = codice
    data["data_inizio"] = row["data_inizio"]
    data["sigma"] = row["sigma"]
    data["ambiente"] = ambiente

    return update_DB_pagine_gara(row["link_gara"], codice, data, conn)


def get_events_link(conn: Connection, update_condition: str, where_clause=""):
    """
    Cerca i link a iscritti/turni iniziali/risultati delle gare della tabella
    gare. Pagine gare di esempio per sigma vecchissimo, vecchio e nuovo
    https://www.fidal.it/risultati/2025/REG37891/Index.htm standard
    https://www.fidal.it/risultati/2011/REG1806/Index.htm con Diffr
    https://www.fidal.it/risultati/2020/REG22800/RESULTSBYEVENT1.htm 1 pagina
    https://www.fidal.it/risultati/2022/REG28833/RESULTSBYEVENT1.htm 3 pagine
    https://www.fidal.it/risultati/2025/REG38222/Risultati/IndexRisultatiPerGara.html

    Salva anche il nome con cui compare quella disciplina.

    conn:             connessione al database
    update_condition: 'date_N'    per controllare solo le gare nell'intorno di N
                                  giorni o non controllate dopo almeno N giorni
                      'scrape_M'  per controllare le gare di oggi che non sono
                                  state controllate da più di M minuti
                      'all'       per controllare tutto il database (non NULL)
                      'custom'    usa la where_clause in input
    """

    num_new_rows = 0
    todayis = datetime.now().astimezone().date()

    if update_condition.startswith("date_"):
        time_span = int(update_condition.split("_")[1])  # giorni
        print(f"Controllo gare finite da al massimo {time_span} giorni")
        where_clause = f"""
            WHERE ABS(DATE '{todayis}' - data_inizio) < {time_span}
               OR (
                   link_iscritti IS NOT NULL
                   AND (
                       scraped_link_iscritti IS NULL
                       OR scraped_link_iscritti < data_fine + ({time_span} * INTERVAL '1 day')
                   )
               )
               OR (
                   link_risultati IS NOT NULL
                   AND (
                       scraped_link_risultati IS NULL
                       OR scraped_link_risultati < data_fine + ({time_span} * INTERVAL '1 day')
                   )
               )
        """

    elif update_condition.startswith("scrape_"):
        minutes = int(
            update_condition.split("_")[1]
        )  # quanti minuti fa è stato fatto lo scraping
        print(f"Controllo gare non controllate da più di {minutes} minuti")

        where_clause = f"""WHERE
            status IN ('iscritti', 'risultati')
            AND DATE '{todayis}' BETWEEN data_inizio AND data_fine
            AND (
                scraped_link_risultati IS NULL OR
                scraped_link_risultati < (CURRENT_TIMESTAMP - INTERVAL '{minutes} minutes')
            )
        """

    elif update_condition == "custom":
        if where_clause == "":
            print("where_clause vuota, non aggiornerò tutto il DB")
            return
        print("Controllo gare filtrate con:")
        print(where_clause)

    else:
        print(
            f"Update criteria '{update_condition}' not valid. "
            f"Valid one are 'ok' and 'date_N' where N is an integer"
        )
        return

    query = f"SELECT * FROM gare {where_clause}"
    df_gare = pd.read_sql(query, conn)

    if df_gare.empty:
        print("Non ci sono gare da controllare")
        return
    else:
        print(f"Aggiorno i link di {len(df_gare)} gare")

    ## Link al sigma NUOVO
    df_nuovo = df_gare[(df_gare["sigma"] == "nuovo")].reset_index(drop=True)
    tot = str(len(df_nuovo))

    if int(tot) == 0:
        print("Non ci sono link al sigma nuovo da aggiornare")

    else:
        print("\nAnalizzo i link al sigma nuovo:\n")

        for ii, (_, row) in enumerate(df_nuovo.iterrows(), start=1):
            print(f"\t{ii + 1}/{tot}", end="\r")
            num_new_rows += link_sigma_nuovo(row, conn)

    ## Link al sigma VECCHIO
    df_vecchio = df_gare[df_gare["sigma"] == "vecchio"].reset_index(drop=True)
    tot = str(len(df_vecchio))
    if int(tot) == 0:
        print("Non ci sono link al sigma vecchio da aggiornare")

    else:
        print("\nAnalizzo i link al sigma vecchio:\n")

        for ii, row in df_vecchio.iterrows():
            print(f"\t{ii + 1}/{tot}", end="\r")
            num_new_rows += link_risultati_sigma_vecchio(row, conn)

    ## Link al sigma VECCHISSIMO
    df_vecchissimo = df_gare[(df_gare["sigma"] == "vecchissimo")].reset_index(drop=True)

    tot = str(len(df_vecchissimo))
    if int(tot) == 0:
        print("Non ci sono link al sigma vecchissimo da aggiornare")

    else:
        print("\nAnalizzo i link al sigma vecchissimo:\n")

        for ii, row in df_vecchissimo.iterrows():
            print(f"\t{ii + 1}/{tot}", end="\r")
            num_new_rows += link_risultati_sigma_vecchissimo(row, conn)

    print(f"{num_new_rows} link aggiunti")


################################################################################
# There is no way this problem can be deterministically approached and I       #
# decided I'll just never try to do it again. What follows is the result of    #
# days of madness.                                                             #
# The only good thing that came out of it is that I wrote so much code in one  #
# day my right wrist hurt so much I figured learning vim was a good thing.     #
################################################################################


def assegna_evento_generale(nome_evento, gara):
    """Mi fido del me stesso di qualche anno fa, non ho intenzione di
    controllare quesa funzione"""
    ## dato un nome di un evento n, come appare nella pagina della gara, gli assegna una categoria generale:
    ## 'altro','peso','disco','martello','giavellotto','pallina','palla','vortex','asta','lungo da fermo'
    ## 'lungo','alto','triplo','quadruplo','ostacoli','marcia','staffetta','corsa piana','prove multiple','boh'
    ## restituisce il nome della categoria e un'altra stringa con i possibili warning

    nome_evento = (
        nome_evento.lower()
        .replace("finale", "")
        .replace("finali", "")
        .replace("batterie", "")
        .replace("mt.", "")
        .replace("metri", "")
        .strip()
    )
    nome_evento = (
        nome_evento.replace("1\u00b0", "").replace("2\u00b0", "").replace("3\u00b0", "")
    )
    evento_generale = ""
    warning_evento = ""
    check = 0

    # Il + spesso compare quando ci sono due disciplina messe assieme.Ma se compare alla fine di solito è per fare riferimento all'età dei master.
    # Se compare più di una volta di solito è perchè concatenano le categorie con il +
    if nome_evento[:-3].count("+") == 1:
        match_plus1 = re.search(
            r"s\d{2}+", nome_evento
        )  # questa parte serve a cercare i più usadi per indicare le categorie master invece
        match_plus2 = re.search(
            r"m\d{2}+", nome_evento
        )  # che un biathlon. In qeusto modo posso diminuire drasticamente il numero di
        match_plus3 = re.search(r"m+\d{2}", nome_evento)  # warning
        match_plus4 = re.search(r"f\d{2}+", nome_evento)
        match_plus5 = re.search(r"f+\d{2}", nome_evento)
        match_plus6 = re.search(r"w+\d{2}", nome_evento)

        pseudo_cat = [
            r"ragazz\w",
            r"cadett\w",
            r"alliev\w",
            r"junior",
            r"juniores",
            r"promesse",
            r"uomini",
            r"donne",
            r"adulti",
            r"senior",
            r"seniores",
        ]
        match_pseudo_cat = False
        for word_sx in pseudo_cat:
            for word_dx in pseudo_cat:
                pattern_cat = word_sx + r"\+" + word_dx

                if re.search(pattern_cat, nome_evento.replace(" ", "")):
                    match_pseudo_cat = True
                    break

            if match_pseudo_cat == True:
                break

        if not (
            match_plus1
            or match_plus2
            or match_plus3
            or match_plus4
            or match_plus5
            or match_plus6
            or match_pseudo_cat
        ):
            warning_evento = "'+' sus"

    # ALTRO
    for word in ["list", "soc", "partecipanti", "risultat"]:
        if gara.startswith(word):
            evento_generale = "altro"
            warning_evento = ""
            return evento_generale, warning_evento
    for word in [
        "modello",
        "classific",
        "complessiv",
        "completi",
        "risultati",
        "1/sta",
        "1 sta",
        "1-sta",
        "1sta",
        "statistic",
        "somma tempi",
        "premio",
        "gran prix",
        "podio",
        "tutti gli",
        "iscritti",
        "iscritti",
        "orario",
        "programma",
        "discpositivo",
        "composizione",
        "start list",
        "elenco",
        "campioni",
        "regolamento",
        "tutti i risultati",
        "modulo",
        "avviso",
        "nota importante",
        "comunicazione",
    ]:
        if word in nome_evento:
            evento_generale = "altro"
            warning_evento = ""
            return evento_generale, warning_evento

    # LANCI
    for word in [
        "peso",
        "disco",
        "martello",
        "giavellotto",
        "pallina",
        "palla",
        "vortex",
        "maniglia",
    ]:
        if word in nome_evento:
            evento_generale = word
            check += 1
            break
    if "discus" in nome_evento:
        evento_generale = "disco"
        check += 1
    if "javelin" in nome_evento:
        evento_generale = "giavellotto"
        check += 1
    if "hammer" in nome_evento:
        evento_generale = "martello"
        check += 1
    if "shot put" in nome_evento:
        evento_generale = "peso"
        check += 1

    # SALTI
    for word in ["asta", "lungo da fermo", "lungo", "triplo", "quadruplo", "alto"]:
        if word in nome_evento.replace("salto", ""):
            evento_generale = word
            check += 1
            break

    if "high jump" in nome_evento:
        evento_generale = "alto"
        check += 1
    if "long jump" in nome_evento:
        evento_generale = "lungo"
        check += 1
    if "triple jump" in nome_evento:
        evento_generale = "triplo"
        check += 1
    if "pole vault" in nome_evento:
        evento_generale = "asta"
        check += 1
    if nome_evento.startswith("pv"):
        evento_generale = "asta"
        check += 1

    # OSTACOLI
    ostacoli = ["ostacoli", " hs ", "hurdle"]
    for word in ostacoli:
        if word in nome_evento:
            evento_generale = "ostacoli"
            check += 1

    pattern_hs = r"\d+hs"
    match_hs = re.search(pattern_hs, nome_evento)
    if match_hs:
        evento_generale = "ostacoli"
        check += 1

    ## MARCIA
    if ("marcia" in nome_evento) | ("race walking" in nome_evento):
        evento_generale = "marcia"
        check += 1

    ## STAFFETTA
    if (
        ("staffetta" in nome_evento)
        | ("staff." in nome_evento)
        | ("relay" in nome_evento)
        | ("realy" in nome_evento)
        | ("giri" in nome_evento)
        | ("giro" in nome_evento)
    ):
        evento_generale = "staffetta"
        check += 1

    if len(nome_evento) > 1:
        if nome_evento[1] == "x":
            evento_generale = "staffetta"
            check += 1

    # Dopo tutto questo dovrei aver pulito abbastanza i nomi da poter fare
    # CORSE
    if check == 0:
        pattern_corse1 = (
            r"^\d+"  # assumo che le corse abbiano sempre la distanza all'inizio
        )
        match_corse1 = re.search(pattern_corse1, nome_evento)
        if match_corse1:
            evento_generale = "corsa piana"
            check += 1

    # PROVE MULTIPLE
    if check == 0:
        # a volte le corse delle multiple hanno 'multiple - 800m', quindi se c'è un numero potrebbe essere una corsa.
        if any(char.isdigit() for char in nome_evento):
            warning_evento = (warning_evento + " " + "PM sus").strip()
        if "thlon" in nome_evento:
            evento_generale = "prove multiple"
            check += 1

    # siepi
    if check == 0 and ("siepi" in nome_evento or "st" in nome_evento):
        evento_generale = "siepi"
        check += 1

    if check > 1:
        warning_evento = (warning_evento + " " + str(check) + " if").strip()

    # Sperando non sia rimasto nulla (falso ho trascurato le siepi, ma siamo indoor per ora)
    if check == 0:
        evento_generale = "boh"

    return evento_generale, warning_evento


def check_master(nome):
    """Mi fido del me stesso di qualche anno fa, non ho intenzione di
    controllare quesa funzione"""
    ## Controlla se l'evento viene taggato come evento master, utilizzato nel caso non siano state trovate altre informazioni
    ## su altezze di ostacoli, massse di pesi/giavellotti/dischi/martelli, distanze
    ## restituisce True o False

    nome = nome.lower()

    match_master0 = re.search(r"master", nome)
    match_master1 = re.search(r"s\d{2}", nome.replace(" ", ""))
    match_master2 = re.search(r"m\d{2}", nome.replace(" ", ""))  # M70+
    match_master3 = re.search(r"sm\d{2}", nome.replace(" ", ""))  # SM45
    match_master4 = re.search(r"f\d{2}", nome.replace(" ", ""))  # F90
    match_master5 = re.search(r"sf\d{2}", nome.replace(" ", ""))  # SF50

    if (
        match_master0
        or match_master1
        or match_master2
        or match_master3
        or match_master4
        or match_master5
    ):
        return True
    else:
        return False


def info_categoria(nome):
    """Mi fido del me stesso di qualche anno fa, non ho intenzione di
    controllare quesa funzione"""
    ## funzione scritta per inferire la categoria di un evento.
    ## DA USARE CON ATTENZIONE
    ## TENERE L'INPUT ORIGINALE CON SPAZI TRA PAROLE
    ## il print di warning avviene solose trova due categorie diverse non senior/promesse
    ## restituisce E, R, CF, CM, AF, AM, JM, AF, AM oppure una stringa vuota
    ## genere per esordienti e ragazzi non è rilevanti.
    ## junior e promesse donne hanno le stesse gare delle assolute

    nome = nome.strip().lower()
    cat = ""
    check = 0

    # Assoluti
    match_hs_ass0 = re.search(r"\badulti u\b", nome)  # adulti u
    match_hs_ass1 = re.search(r"uomini", nome)  # uomini
    match_hs_ass2 = re.search(r"men", nome)  # men
    match_hs_ass3 = re.search(r"maschile", nome)  # maschile
    match_hs_ass4 = re.search(r"\bm\b", nome)  # m
    match_hs_ass5 = re.search(r"\bu\b", nome)  # u
    match_hs_ass6 = re.search(r"\bpromesse u\b", nome)  # promesse u
    match_hs_ass7 = re.search(r"\bpromesse m\b", nome)  # promesse m
    match_hs_ass8 = re.search(r"\bpm\b", nome)  # pm

    match_hs_ass9 = re.search(r"donne", nome)  # donne
    match_hs_ass10 = re.search(r"women", nome)  # women
    match_hs_ass11 = re.search(r"femminile", nome)  # femminile
    match_hs_ass12 = re.search(r"\bf\b", nome)  # f
    match_hs_ass13 = re.search(r"\bd\b", nome)  # d
    match_hs_ass14 = re.search(r"\badulti d\b", nome)  # adulti d
    match_hs_ass15 = re.search(r"\bpromesse d\b", nome)  # promesse d
    match_hs_ass16 = re.search(r"\bpromesse f\b", nome)  # promesse f
    match_hs_ass17 = re.search(r"\bpf\b", nome)  # pf

    if (
        match_hs_ass0
        or match_hs_ass1
        or match_hs_ass2
        or match_hs_ass3
        or match_hs_ass4
        or match_hs_ass5
        or match_hs_ass6
        or match_hs_ass7
        or match_hs_ass8
    ):
        cat = "SM"

    if (
        match_hs_ass9
        or match_hs_ass10
        or match_hs_ass11
        or match_hs_ass12
        or match_hs_ass13
        or match_hs_ass14
        or match_hs_ass15
        or match_hs_ass16
        or match_hs_ass17
    ):
        cat = "SF"

    # Esordienti
    match_eso1 = re.search(r"\besordienti\b", nome)  # esordienti
    match_eso2 = re.search(r"\bef\d+", nome)  # EF8
    match_eso3 = re.search(r"\bem\d+", nome)  # EM5
    match_eso4 = re.search(r"\bef\b", nome)  # EF
    match_eso5 = re.search(r"\bef\b", nome)  # EM

    if match_eso1 or match_eso2 or match_eso3 or match_eso4 or match_eso5:
        check += 1
        cat = "E"

    # Ragazzi
    match_hs_r0 = re.search(r"\bragazz", nome)  # ragazz
    match_hs_r1 = re.search(r"\brm\b", nome)  # rm
    match_hs_r2 = re.search(r"\brf\b", nome)  # rf

    if match_hs_r0 or match_hs_r1 or match_hs_r2:
        cat = "R"
        check += 1

    # Cadetti
    match_hs_c1 = re.search(r"cadetti", nome)  # cadetti
    match_hs_c2 = re.search(r"\bcm\b", nome)  # cm
    match_hs_c3 = re.search(r"cadette", nome)  # cadettte
    match_hs_c4 = re.search(r"\bcf\b", nome)  # cf

    if match_hs_c1 or match_hs_c2:
        cat = "CM"
        check += 1

    if match_hs_c3 or match_hs_c4:
        cat = "CF"
        check += 1

    # Allievi
    match_hs_a1 = re.search(r"allievi", nome)  # allievi
    match_hs_a2 = re.search(r"\bam\b", nome)  # am
    match_hs_a3 = re.search(r"allieve", nome)  # allieve
    match_hs_a4 = re.search(r"\baf\b", nome)  # af

    if match_hs_a1 or match_hs_a2:
        cat = "AM"
        check += 1

    if match_hs_a3 or match_hs_a4:
        cat = "AF"
        check += 1

    # Junior
    match_hs_j1 = re.search(r"junior u", nome)  # junior u
    match_hs_j2 = re.search(r"junior m", nome)  # junior m
    match_hs_j3 = re.search(r"juniores u", nome)  # juniores u
    match_hs_j4 = re.search(r"juniores m", nome)  # juniores m
    match_hs_j5 = re.search(r"\bjm\b", nome)  # jm
    match_hs_j6 = re.search(r"junior d", nome)  # junior d
    match_hs_j7 = re.search(r"junior f", nome)  # junior f
    match_hs_j8 = re.search(r"juniores d", nome)  # juniores d
    match_hs_j9 = re.search(r"juniores f", nome)  # juniores f
    match_hs_j10 = re.search(r"\bjf\b", nome)  # jf

    if match_hs_j1 or match_hs_j2 or match_hs_j3 or match_hs_j4 or match_hs_j5:
        cat = "JF"
        check += 1

    if match_hs_j6 or match_hs_j7 or match_hs_j8 or match_hs_j9 or match_hs_j10:
        cat = "JM"
        check += 1

    if check > 1:
        print(
            "Ho trovato "
            + str(check)
            + "categorie diverse. Restituisco la più giovane. Non fidarti di me!"
        )

    return cat


def info_ostacoli(nome):
    """Mi fido del me stesso di qualche anno fa, non ho intenzione di
    controllare quesa funzione"""
    ## gli ostacoli sono così incasinati che ho dovuto fare una funzione a parte

    nome = nome.lower().replace("finale", "").strip().replace("ostacoli", "hs")
    spec = ""
    warn_spec = ""
    found = False

    # Se non comincia con un numero, faccio solo un guess su quale potrebbe essere la distanza della gara
    if nome[0].isdigit() is False:
        match_dist = re.search(r"\d+", nome)
        if match_dist:
            dist = match_dist[0].strip()
            spec = dist + " Hs"
            warn_spec = "Distanza a caso"
        else:
            spec = "ostacoli"
            warn_spec = "Non conosco la distanza"
        return spec, warn_spec

    # D'ora in poi possiamo assumere che 'nome' cominci con un numero

    # esordienti
    match_eso1 = re.search(r"esordienti", nome)  # esordienti
    match_eso2 = re.search(r"\bef\d+", nome)  # EF8
    match_eso3 = re.search(r"\bem\d+", nome)  # EM5
    match_eso4 = re.search(r"\bef\b", nome)  # EF
    match_eso5 = re.search(r"\bem\b", nome)  # EM
    match_eso6 = re.search(r"\bef\w\b", nome)  # EFA
    match_eso7 = re.search(r"\bem\w\b", nome)  # EMB

    if not (found) and (
        match_eso1
        or match_eso2
        or match_eso3
        or match_eso4
        or match_eso5
        or match_eso6
        or match_eso7
    ):
        dist = re.search(r"\d+", nome)[0]
        spec = dist.strip() + " Hs Esordienti"
        found = True

    # master
    if not (found) and check_master(nome):
        spec = re.search(r"\d+", nome)[0].strip() + " Hs Master"
        found = True

    # togliamoci dai piedi quelli scritti bene
    pat_hs0 = r"\d+hsh\d+-\d.\d{2}"  # 60hsh106-9.14
    match_hs0 = re.search(pat_hs0, nome.replace(" ", ""))

    if not (found) and match_hs0:
        spec = (
            match_hs0[0].strip().split("h")[0]
            + " Hs h"
            + match_hs0[0].strip().split("h")[2][:-5]
        )
        found = True

    # passiamo a quelli scritti senza distanza
    match_hs1 = re.search(r"h\d+", nome.replace(" ", ""))  # h100

    if not (found) and match_hs1:
        dist = re.search(r"\d+[^\d]*hs", nome.replace(" ", ""), re.IGNORECASE)[
            0
        ]  # match full "number-junk-hs"
        dist = re.search(r"\d+", dist)[0]  # extract only the number
        h = match_hs1[0].strip().split("h")[-1]

        spec = dist + " Hs h" + h
        found = True

    # ora devo indentificare le categorie se voglio sapere l'altezza dell'ostacolo
    # ragazzi
    dist = re.search(r"\d+", nome)[0].strip()
    match_hs_r0 = re.search(r"ragazz", nome)  # ragazz
    match_hs_r1 = re.search(r"\brm\b", nome)  # rm
    match_hs_r2 = re.search(r"\brf\b", nome)  # rf

    if not (found) and (match_hs_r0 or match_hs_r1 or match_hs_r2):
        spec = dist + " Hs h60"
        found = True

    # cadetti e cadette
    match_hs_c1 = re.search(r"cadetti", nome)  # cadetti
    match_hs_c2 = re.search(r"\bcm\b", nome)  # cm
    match_hs_c3 = re.search(r"cadette", nome)  # cadettte
    match_hs_c4 = re.search(r"\bcf\b", nome)  # cf

    if not (found) and (match_hs_c1 or match_hs_c2):
        if dist in ("60", "100"):
            h = "84"
        elif dist in ("200", "300"):
            h = "76"
        else:
            warn_spec = "distanza strana"
            h = ""
        spec = dist + " Hs h" + h
        found = True

    if not (found) and (match_hs_c3 or match_hs_c4):
        spec = dist + " Hs h76"
        found = True

    # allievi e allieve
    match_hs_a1 = re.search(r"allievi", nome)  # allievi
    match_hs_a2 = re.search(r"\bam\b", nome)  # am
    match_hs_a3 = re.search(r"allieve", nome)  # allieve
    match_hs_a4 = re.search(r"\baf\b", nome)  # af

    if not (found) and (match_hs_a1 or match_hs_a2):
        if dist in ("60", "100"):
            h = "91"
        elif dist == "200":
            h = "76"
        elif dist == ("300", "400"):
            h = "84"
        else:
            warn_spec = "distanza strana"
            h = ""
        spec = dist + " Hs h" + h
        found = True

    if not (found) and (match_hs_a3 or match_hs_a4):
        spec = dist + " Hs h76"
        found = True

    # junior
    match_hs_j1 = re.search(r"junior u", nome)  # junior u
    match_hs_j2 = re.search(r"junior m", nome)  # junior m
    match_hs_j3 = re.search(r"juniores u", nome)  # juniores u
    match_hs_j4 = re.search(r"juniores m", nome)  # juniores m
    match_hs_j5 = re.search(r"\bjm\b", nome)  # jm
    match_hs_j6 = re.search(r"junior d", nome)  # junior d
    match_hs_j7 = re.search(r"junior f", nome)  # junior f
    match_hs_j8 = re.search(r"juniores d", nome)  # juniores d
    match_hs_j9 = re.search(r"juniores f", nome)  # juniores f
    match_hs_j10 = re.search(r"\bjf\b", nome)  # jf

    if not (found) and (
        match_hs_j1 or match_hs_j2 or match_hs_j3 or match_hs_j4 or match_hs_j5
    ):
        if dist in ("60", "110"):
            h = "100"
        elif dist == "200":
            h = "76"
        elif dist == ("300", "400"):
            h = "91"
        else:
            warn_spec = "distanza strana"
            h = ""
        spec = dist + " Hs h" + h
        found = True

    if not (found) and (
        match_hs_j6 or match_hs_j7 or match_hs_j8 or match_hs_j9 or match_hs_j10
    ):
        if dist in ("60", "100"):
            h = "84"
        elif dist == "200":
            h = "76"
        elif dist == ("300", "400"):
            h = "76"
        else:
            warn_spec = "distanza strana"
            h = ""
        spec = dist + " Hs h" + h
        found = True

    # In teoria mi sono rimasti solo gli assoluti ora. Devo solo distinguere tra uomo e donna
    match_hs_ass1 = re.search(r"uomini", nome)  # uomini
    match_hs_ass2 = re.search(r"men", nome)  # men
    match_hs_ass3 = re.search(r"maschil\w", nome)  # maschile
    match_hs_ass4 = re.search(r"\bm\b", nome)  # m
    match_hs_ass5 = re.search(r"\bu\b", nome)  # u
    match_hs_ass6 = re.search(r"donne", nome)  # donne
    match_hs_ass7 = re.search(r"women", nome)  # women
    match_hs_ass8 = re.search(r"femminil\w", nome)  # maschile
    match_hs_ass9 = re.search(r"\bf\b", nome)  # f
    match_hs_ass10 = re.search(r"\bd\b", nome)  # d

    if not (found) and (
        match_hs_ass1
        or match_hs_ass2
        or match_hs_ass3
        or match_hs_ass4
        or match_hs_ass5
    ):
        if dist in ("60", "110"):
            h = "106"
        elif dist == "200":
            h = "76"
        elif dist == ("300", "400"):
            h = "91"
        else:
            warn_spec = "distanza strana"
            h = ""
        spec = dist + " Hs h" + h
        warn_spec = "a esclusione"
        found = True

    if not (found) and (
        match_hs_ass6
        or match_hs_ass7
        or match_hs_ass8
        or match_hs_ass9
        or match_hs_ass10
    ):
        if dist in ("60", "100"):
            h = "84"
        elif dist == "200":
            h = "76"
        elif dist == ("300", "400"):
            h = "76"
        else:
            warn_spec = "distanza strana"
            h = ""
        spec = dist + " Hs h" + h
        warn_spec = "a esclusione"
        found = True

    if not (found):
        spec = dist + " Hs"
        warn_spec = "non conosco l'altezza"

    # if check > 2:
    #    warn_spec = 'sus, ho trovato '+str(check)+' pattern'

    return spec, warn_spec


def assegna_evento_specifico(nome, eve):
    """Mi fido del me stesso di qualche anno fa, non ho intenzione di
    controllare quesa funzione"""
    ## dato il nome dell'evento che compare nella pagina della gara e la categoria generale che gli è stata assegnata da
    ## asssegna_evento_generale() analizza il nome dell'evento per ottenere maggiori informazioni (altezza ostacoli, categoria
    ## massa di peso, disco, martello e giavellotto)
    ## usa info_ostacoli() e check_master()
    ## ritorna l'evento specifico e una stringa con possibli warning

    check = 0  # serve a controllare se per motivi scemi l'evento specifico viene scritto due volte
    nome = nome.lower().replace("finale", "").replace("finale", "").strip()
    nome = nome.replace("1\u00b0", "").replace("2\u00b0", "").replace("3\u00b0", "")
    spec = ""  # evento specifico
    warn_spec = ""

    # qualche utile pattern per i master

    # SALTI e VORTEX, evento generale: ['asta','lungo da fermo','lungo','alto','triplo','quadruplo']
    if eve == "altro":
        spec = "altro"
    elif eve == "asta":
        spec = "asta"
    elif eve == "lungo da fermo":
        spec = "lungo da fermo"
    elif eve == "lungo":
        spec = "lungo"
    elif eve == "alto":
        spec = "alto"
    elif eve == "triplo":
        spec = "triplo"
    elif eve == "quadruplo":
        spec = "quadruplo"
    elif eve == "vortex":
        spec = "Vortex"

    # CORSE

    elif re.search(r"1miglio", nome.replace(" ", "")):
        spec = "1 Miglio"  # evviva le freedom units
    elif re.search(r"2miglia", nome.replace(" ", "")):
        spec = "2 Miglia"
    elif eve == "corsa piana":
        nome = nome.replace(" ", "")

        pat_corse = r"^\d+"  # assumo che le corse abbiano sempre la distanza all'inizio
        match_corse = re.search(pat_corse, nome)

        if match_corse:
            spec = match_corse[0].strip() + "m"

    # STAFFETTA

    elif eve == "staffetta":
        nome = nome.replace(" ", "")

        if "giro" in nome:
            nome = nome.replace("1giro", "200").replace("1 giro", "200")
        if "giri" in nome:
            nome = nome.replace("2giri", "400").replace("2 giri", "400")

        pat_staff1 = r"\d+x\d+"  # 4x100
        pat_staff2 = r"\d+ x \d+"  # 4 x 100
        match_staff1 = re.search(pat_staff1, nome)
        match_staff2 = re.search(pat_staff2, nome)

        if match_staff1:
            spec = match_staff1[0].strip() + "m"
            check += 1

        if match_staff2:
            spec = match_staff2[0].strip().replace(" ", "") + "m"
            check += 1

        if check == 0:
            spec = "staffetta"
            warn_spec = "non conosco la staffetta"

        if check == 2:
            warn_spec = "sus, ho trovato entrambi i pattern"

    # MARCIA

    elif eve == "marcia":
        nome = nome.replace(" ", "")

        match_marcia1 = re.search(r"\d+m", nome)  # 3000m
        match_marcia2 = re.search(r"\d+km", nome)  # 3km
        match_marcia3 = re.search(r"km\d+", nome)  # km3

        if match_marcia1:
            spec = "Marcia " + match_marcia1[0].strip()
            check += 1

        if match_marcia2:
            spec = "Marcia " + match_marcia2[0][:-2].strip() + "000m"
            check += 1

        if match_marcia3:
            spec = "Marcia " + match_marcia3[0][2:].strip() + "000m"
            check += 1

        if check == 0:
            if check_master(nome):
                spec = "Marcia Master"
            else:
                spec = "Marcia"
                warn_spec = "non conosco la distanza"

        if check == 2:
            warn_spec = "sus, ho trovato entrambi i pattern"

    # DISCO

    elif eve == "disco":
        nome = nome.replace(" ", "")

        pat_disco = r"kg\d+.\d+"
        match_disco = re.search(pat_disco, nome)

        if match_disco:
            spec = "Disco " + match_disco[0][2:].strip() + "Kg"
            check += 1

        if check == 0:
            if check_master(nome):
                spec = "Disco Master"
            else:
                spec = "Disco"
                warn_spec = "non conosco la massa"

    # GIAVELLOTTO

    elif eve == "giavellotto":
        nome = nome.replace(" ", "")

        pat_giav1 = r"g\d+"  # g400
        pat_giav2 = r"gr\d+"  # gr400
        pat_giav3 = r"\d+g"  # 400g
        match_giav1 = re.search(pat_giav1, nome)
        match_giav2 = re.search(pat_giav2, nome)
        match_giav3 = re.search(pat_giav3, nome)

        if match_giav1:
            spec = "Giavellotto " + match_giav1[0][1:].strip() + "g"
            check += 1

        if match_giav2:
            spec = "Giavellotto " + match_giav2[0][2:].strip() + "g"
            check += 1

        if match_giav3:
            spec = "Giavellotto " + match_giav3[0][:-1].strip() + "g"
            check += 1

        if check == 0:
            if check_master(nome):
                spec = "Giavellotto Master"
            else:
                spec = "Giavellotto"
                warn_spec = "non conosco la massa"

        if check > 1:
            warn_spec = "sus, ho trovato tanti i pattern"

    # MARTELLO

    elif eve == "martello":
        nome = nome.replace(" ", "")

        pat_mart = r"kg\d+.\d+"
        match_mart = re.search(pat_mart, nome)

        if match_mart:
            spec = "Martello " + match_mart[0][2:].strip() + "Kg"
            check += 1

        if check == 0:
            if check_master(nome):
                spec = "Martello Master"
            else:
                spec = "Martello"
                warn_spec = "non conosco la massa"

    # PESO
    # Da migliorare, ma ancora non sono sicuro di come fare e non ho voglia di pensarci

    elif eve == "peso":
        nome = nome.replace(" ", "")

        match_peso1 = re.search(
            r"kg\d+", nome
        )  # kg5               Nota: l'ordine è importante perchè
        match_peso2 = re.search(
            r"kg\d+.\d+", nome
        )  # kg5.000           la stringa 1 e 3 si può anche trovare
        match_peso3 = re.search(
            r"\d+kg", nome
        )  # 5kg               nella stringa 2 e 4. Quindi i match a
        match_peso4 = re.search(
            r"\d+.\d+kg", nome
        )  # 5.000kg           2 e 4 devono andare dopo per sovraiscrivere

        if match_peso1:
            spec = "Peso " + match_peso1[0][2:].strip() + ".000Kg"
            check += 1

        if match_peso2:
            spec = "Peso " + match_peso2[0][2:].strip() + "Kg"
            check += 1

        if match_peso3:
            spec = "Peso " + match_peso3[0][:-2].strip() + ".000Kg"
            check += 1

        if match_peso4:
            spec = "Peso " + match_peso4[0][:-2].strip() + "Kg"
            check += 1
        if "7" in spec:
            spec = "Peso 7.260Kg"
        if check == 0:
            if check_master(nome):
                spec = "Peso Master"
            else:
                spec = "Peso"
                warn_spec = "non conosco la massa"

        if check > 2:  # dovrebbe essere >1, ma guarda la nota sopra
            warn_spec = "sus, ho trovato tanti i pattern"

    # OSTACOLI
    elif eve == "ostacoli":
        (spec, warn_spec) = info_ostacoli(nome)

    # SIEPI
    elif eve == "siepi":
        nome = nome.replace("st", "siepi").replace(" ", "")
        warn_spec = "siepi non ancora implementate"

    else:
        spec = eve

    return spec, warn_spec


def assegna_evento_sigma_nuovo(row, conn):
    """ """

    gara = row["gara"]
    match1 = re.match(r"Gara\d{3}\.htm", gara)
    match2 = re.match(r"Diffr.*\.htm", gara)

    # Faremo solo i risultati
    if (match1 is None) and (match2 is None):
        query = text(f"""UPDATE pagine_gara
                     SET disciplina = 'iscrizione'
                     WHERE id = {row["id"]}""")
        conn.execute(query)
        conn.commit()
        return

    url = f"{DOMAIN}{row['anno']}/{row['codice']}/Risultati/{gara}"
    try:
        r = requests.get(url)
        if r.status_code != 200:
            print("Link rotto", url)
            return

        soup = BeautifulSoup(r.text, "html.parser")
        div = soup.find("div", class_="col-md-4")
        p = div.find("p", class_="h4 text-danger mb-4 mt-4")
        span = p.find("span", class_="h7 text-danger")
        disciplina = span.text[2:].strip()  # finally

        query = text(f"""UPDATE pagine_gara
                     SET disciplina = :disciplina
                     WHERE id = {row["id"]}""")
        conn.execute(query, {"disciplina": disciplina})
        conn.commit()
    except:
        print("Qualcosa è andato storto:", url)
        return


def assegna_evento(conn, update_condition, where_clause=""):
    """
    Wrapper che applica le GOATED assegna_evento_generale() e
    assegna_evento_specifico() al database secondo
    update_condition: 'null' aggiorna tutte le righe che hanno disciplina null
                      'custom' usa la where_clause in input
    assegna anche lo status di iscritti/start list/risultato
    """

    if update_condition == "null":
        where_clause = "WHERE disciplina IS NULL"

    elif update_condition.startswith("anno_"):
        anno = int(
            update_condition.split("_")[1]
        )  # quanti giorni dopo la gara continuo a cercare risultati
        print(f"Controllo gare del {anno}")
        where_clause = f"""
                WHERE disciplina IS NULL
                AND anno = '{anno}'
            """
    elif update_condition == "custom":
        print("Uso:")
        print(where_clause)
    else:
        print("Non conosco l'update_condition", update_condition)
        return

    query = f"SELECT * FROM pagine_gara {where_clause}"
    df = pd.read_sql_query(query, conn)

    ## Nomi per sigma vecchio e vecchissimo
    df_old = df[df["sigma"] != "nuovo"].reset_index(drop=True)
    tot = len(df_old)
    for ii, row in df_old.iterrows():
        print(f"\t{ii:d}/{tot:d}", end="\r")
        event_gen, warn_gen = assegna_evento_generale(row["nome"], row["gara"])
        event_spec, warn_spec = assegna_evento_specifico(row["nome"], event_gen)

        update_query = text("""
            UPDATE pagine_gara SET
                disciplina = :disciplina,
                warn_gen = :warn_gen,
                warn_spec = :warn_spec
            WHERE id = :id
        """)
        conn.execute(
            update_query,
            {
                "disciplina": event_spec or None,
                "warn_gen": warn_gen or None,
                "warn_spec": warn_spec or None,
                "id": row["id"],
            },
        )

    conn.commit()

    ## Nomi per sigma nuovo
    df_new = df[df["sigma"] == "nuovo"].reset_index(drop=True)
    tot = len(df_new)
    print("sigma nuovo", tot)
    for ii, row in df_new.iterrows():
        print(f"\t{ii:d}/{tot:d}", end="\r")
        assegna_evento_sigma_nuovo(row, conn)


def hard_strip(nome_str) -> str:
    """
    LEGACY: costruite le funzioni assegna_evento_generale() e
    assegna_evento_specifico() che fanno un lavoro molto milgiore
    prende il nome dato alla disciplina di una gara e fa del suo meglio per
    togliere tutte le cose inutili cercando di lasciare solo le informazioni
    importanti.
    Indescrivibile il mio odio per questa cosa. Evviva il sigma nuovo che mette
    la disciplina corretta nella pagina.
    """

    nome_str = nome_str.strip().lower()

    starting_trash = ["modello 1", "modello", "risultati", "classifica"]
    for word in starting_trash:
        if nome_str.startswith(word):
            return "altro"

    nome_str = nome_str.replace("(", " ").replace(")", " ")

    first_trash = [
        "finale",
        "extra",
        "ad invito",
        "invito",
        "piani",
        "staffetta",
        "staff.",
        " u14 ",
        " u15 ",
        " u16 ",
        " u17 ",
        " u18 ",
        " u19 ",
        " u19 ",
        " u20 ",
        " u23 ",
    ]
    for word in first_trash:
        nome_str = nome_str.replace(word, "")

    nome_str = nome_str.replace("metri", "m").replace("ostacoli", "hs")

    second_trash = ["salto con l'", "salto in", "salto"]
    for word in second_trash:
        if nome_str.startswith(word):
            nome_str = nome_str.replace(word, "")

    third_trash = [" ", "/", "bancari", "indoor"]
    for word in third_trash:
        nome_str = nome_str.replace(word, "")

    fourth_trash = ["asta", "lungodafermo", "lungo", "alto", "triplo", "quadruplo"]
    for word in fourth_trash:
        if nome_str.startswith(word):
            return word

    fifth_trash = [
        "pesospkg1",
        "pesospkg2",
        "pesospkg3",
        "pesospkg4",
        "pesospkg5",
        "pesospkg6",
        "pesospkg7.260",
    ]
    for word in fifth_trash:
        if nome_str.startswith(word):
            return word.replace("sp", "")

    sixth_trash = [
        "pesokg1",
        "pesokg2",
        "pesokg3",
        "pesokg4pesokg5",
        "pesokg6",
        "pesokg7.260",
    ]
    for word in sixth_trash:
        if nome_str.startswith(word):
            return word

    return nome_str
