## Procedimento

Si parte da ```calendario.py``` --> ```codici_gare``` \
--> ```risultati_gara.py``` --> ```link_risultati``` -> ```scraping_risultati``` \
--> ```get_rankings.py``` -> ```rankings```

#### Link al calendario \
https://www.fidal.it/calendario.php?anno=2025&mese=1&livello=REG&new_regione=VENETO&new_tipo=3&new_categoria=PRO&submit=Invia

#### Parametri
 - mese: 
    - gennatio = 1
    - febbraio = 2
    - ... 

 - livello
    - nessuno = 
    - regionale = REG
    - nazionele = COD

- new_regione
    - venero = VENETO
    - ...

- new_tipo
    - nessuno = 0
    - cross = 2
    - indoor = 3
    - outdoor = 5
    - strada = 6
    - piazza = 10
    - ...

- new_categoria
    - nessuno = 
    - esordienti = ESO
    - ragazzi = RAG
    - cadetti = CAD
    - allievi = ALL
    - junior = JUN
    - promesse = PRO
    - seniores = SEN
    - master = MAS

Ogni gara è identificata da un codice REGXXXXX che si ottine dal file HTML del calendario con calendario.py
(salvato in codici_gare). Una volta ottenuto il codice il link ai risultati della gara è uno dei seguenti:

 - https://www.fidal.it/risultati/2024/REGXXXXX/Risultati/IndexRisultatiPerGara.html
 - https://www.fidal.it/risultati/2024/REGXXXXX/RESULTSBYEVENT1.htm

In base alla versione di sigma che usa la regione.

Da qui comincia il caos perché i link con i risultati della singola disciplina sono del tipo

https://www.fidal.it/risultati/2024/REG33797/Risultati/Gara722.html

solo che il numero 722 che indica in questo caso i 60HS uomini è abbastanza arbitrario e varia di gara in gara.
Quindi bisogna cercare il link dei risultati specifici in base al nome del bottone es "60Hs H106" o "60m UOMINI".
Solo il sistema SIGMA nuovo ha un elemento html ```class='h7 text-danger'``` che a volte contiene un nome 
univoco che identifica la disciplina.
Questo rende in codice fragile nell'identificare la disciplina delle gare che usano il sistema vecchio del
sigma.

```risultati_gara.py``` prende i codici gara (es. REG33797) e capisce se deve usare la versione di sigma
vecchia o nuova.
Poi data una disciplina fornisce direttamente il link ai risultati di quella disciplina.
ATTENZIONE: la disciplina viene letteramente cercata con una ricerca testuale, quindi se invece che scrivere
```60Hs H106``` gli organizzatori hanno scritto solo ```60HS``` il programma non può determinare in modo
automatico la disciplina corretta. A questo scopo è stato fatto un grosso lavoro di filtri
(```func_assegnazione_evento.py```) che permette di trovare la disciplina corretta nel 99% dei casi e di
lasciare un warning quando non è sicuro. 

```scraping_risultati.py``` si occupa di prendere i risultati dal link della pagina e restituire una tabella
unica.
NOTA: per motivi tecnici due risultati uguali fatti nello steso giorno e caricati nel sigma nuovo vengono
visti come duplicati ed eliminati, non sapevo come altro fare.

```get_ranking.py``` si occuperà ora di ottenere i ranking 

## TODO
 - Ottenere ogni link risultati di ogni gara con a fianco il nome dell'evento che viene utilizzato
 - Prendere data e luogo dalla pagina dei risultati e aggiungerla alla risultati_gara
 - Aggiungere il codice gara/nome della gara
