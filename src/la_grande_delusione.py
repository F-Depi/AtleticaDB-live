from func_general import (
    assegna_evento,
    get_db_engine,
)

"""
Aggiornamento del 2026-10-07 
Tutte le regioni sembrano effettivamente passate al nuovo sigma, che in realtà
sarebbe WISE. La label a fianco a risultati rimane arbitraria, ma secondo me
c'è un buona possibilità di riuscire a fare qualcosa accurato al 99%.
In ogni caso l'identificazione esatta della disciplina non è più necessaria nel
nuovo progetto
"""



"""
Identifichiamo la disciplina corretta con il dizionari dei nomi

                            
                            La Grande Delusione                                

Identificare la disciplina corretta è l'inferno. Il nome a una pagina di
risultati di una gara viene dato in modo molto arbitrario e fantasioso.
Gli sforzi per identificare in modo sistematico la disciplina esatta in una gara
con sigma vecchio e vecchissimo sono stati immani e, per quanto i risultati
siano sembrati a momenti promettenti, non ritengo di aver raggiunto
un'accuratezza che mi permetterebbe di dormire tranquillo la notte.
C'era però una luce in fondo al tunnel. Il sigma nuovo presenta in ogni pagina
di risultati, a fianco alla scritta risultati, una label dall'aspetto molto 
ufficiale riguardo la disciplina a cui sono associati quei risultati.
Pensavo, sognavo, che quella disciplina fosse generata automaticamente dal 
sistema e mi desse informazioni esatte sulla disciplina. Immaginavo un futuro
in cui tutte le regioni sarebbero finalmente passate al sigma nuovo e il 
problema di identificare la disciplina si sarebbe risolto.
Ahimé, ahi noi, è stato un abbaglio. Anche quella label, che sembra una
disciplina ufficiale, non è altro che una scritta messa a mano dai giudici...
arbitraria, ambigua, talvolta addirittura errata...
Non c'è mai stata una luce in fondo al tunnel
7324 nomi distinti su 53069
"""
print("\n---------------------------------------------")
print("Assegno l'evento ai risultati")
update_condition = "anno_2025"
with get_db_engine().connect() as conn:
    assegna_evento(conn, update_condition)


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
