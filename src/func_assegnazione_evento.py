import re

def assegna_evento_generale(nome_evento, link):
    
    ## dato un nome di un evento n, come appare nella pagina della gara, gli assegna una categoria generale:
    ## 'altro','peso','disco','martello','giavellotto','pallina','palla','vortex','asta','lungo da fermo'
    ## 'lungo','alto','triplo','quadruplo','ostacoli','marcia','staffetta','corsa piana','prove multiple','boh'
    ## restituisce il nome della categoria e un'altra stringa con i possibili warning
    
    link = link.split('/')[-1].lower()
    nome_evento = nome_evento.lower().replace('finale','').replace('finali','').replace('batterie','').replace('mt.','').replace('metri','').strip()
    nome_evento = nome_evento.replace('1\u00b0','').replace('2\u00b0','').replace('3\u00b0','')
    evento_generale = ''
    warning_evento = ''
    check = 0
    
    # Il + spesso compare quando ci sono due disciplina messe assieme.Ma se compare alla fine di solito è per fare riferimento all'età dei master.
    # Se compare più di una volta di solito è perchè concatenano le categorie con il +
    if nome_evento[:-3].count('+') == 1:
        
        match_plus1 = re.search(r's\d{2}+', nome_evento)   # questa parte serve a cercare i più usadi per indicare le categorie master invece 
        match_plus2 = re.search(r'm\d{2}+', nome_evento)   # che un biathlon. In qeusto modo posso diminuire drasticamente il numero di 
        match_plus3 = re.search(r'm+\d{2}', nome_evento)   # warning
        match_plus4 = re.search(r'f\d{2}+', nome_evento)
        match_plus5 = re.search(r'f+\d{2}', nome_evento)
        match_plus6 = re.search(r'w+\d{2}', nome_evento)

        pseudo_cat = [r'ragazz\w',r'cadett\w',r'alliev\w',r'junior',r'juniores',r'promesse',r'uomini',r'donne',r'adulti',r'senior',r'seniores']
        match_pseudo_cat = False
        for word_sx in pseudo_cat:
            for word_dx in pseudo_cat:
                
                pattern_cat = word_sx + r'\+' + word_dx

                if re.search(pattern_cat, nome_evento.replace(' ','')):
                    match_pseudo_cat = True
                    break
            
            if match_pseudo_cat == True:
                break
        
        if not (match_plus1 or match_plus2 or match_plus3 or match_plus4 or match_plus5 or match_plus6 or match_pseudo_cat):
            warning_evento = '\'+\' sus'
    
    # ALTRO
    for word in ['list','soc','partecipanti','risultat']:
        if link.startswith(word):
            evento_generale = 'altro'
            warning_evento= ''
            return evento_generale, warning_evento
    for word in ['modello','classific','complessiv','completi','risultati','1/sta','1 sta','1-sta','1sta',
                 'statistic','somma tempi','premio','gran prix','podio','tutti gli','iscritti','iscrizioni',
                 'orario','programma','discpositivo','composizione','start list','elenco','campioni',
                 'regolamento','tutti i risultati','modulo','avviso','nota importante','comunicazione']:  
        if word in nome_evento:
            evento_generale = 'altro'
            warning_evento= ''
            return evento_generale, warning_evento
    
    # LANCI
    for word in ['peso','disco','martello','giavellotto','pallina','palla','vortex','maniglia']:
        if word in nome_evento:
            evento_generale = word
            check = check + 1
            break
    if 'discus' in nome_evento:
        evento_generale = 'disco'
        check = check + 1
    if 'javelin' in nome_evento:
        evento_generale = 'giavellotto'
        check = check + 1
    if 'hammer' in nome_evento:
        evento_generale = 'martello'
        check = check + 1
    if 'shot put' in nome_evento:
        evento_generale = 'peso'
        check = check + 1
        
    # SALTI
    for word in ['asta','lungo da fermo','lungo','triplo','quadruplo','alto']:
        if word in nome_evento.replace('salto', ''):
            evento_generale = word
            check = check + 1
            break
        
    if 'high jump' in nome_evento:
        evento_generale = 'alto'
        check = check + 1
    if 'long jump' in nome_evento:
        evento_generale = 'lungo'
        check = check + 1
    if 'triple jump' in nome_evento:
        evento_generale = 'triplo'
        check = check + 1
    if 'pole vault' in nome_evento:
        evento_generale = 'asta'
        check = check + 1
    if nome_evento.startswith('pv'):
        evento_generale = 'asta'
        check = check + 1 

    # OSTACOLI
    ostacoli = ['ostacoli',' hs ','hurdle']
    for word in ostacoli:
        if word in nome_evento:
            evento_generale = 'ostacoli'
            check = check + 1
    
    pattern_hs = r'\d+hs'
    match_hs = re.search(pattern_hs, nome_evento)
    if match_hs:
        evento_generale = 'ostacoli'
        check = check + 1
        
    ## MARCIA
    if ('marcia' in nome_evento) | ('race walking' in nome_evento):
        evento_generale = 'marcia'
        check = check + 1
        
    ## STAFFETTA
    if ('staffetta' in nome_evento) | ('staff.' in nome_evento) | ('relay' in nome_evento) | ('realy' in nome_evento) | (nome_evento[1] == 'x') | ('giri' in nome_evento) | ('giro' in nome_evento):
        
        evento_generale = 'staffetta'
        check = check + 1    
    
    # Dopo tutto questo dovrei aver pulito abbastanza i nomi da poter fare
    # CORSE
    if check == 0:
        pattern_corse1 = r'^\d+' # assumo che le corse abbiano sempre la distanza all'inizio
        match_corse1 = re.search(pattern_corse1, nome_evento)
        if match_corse1:
            evento_generale = 'corsa piana'
            check = check + 1

    # PROVE MULTIPLE
    if check == 0:
        # a volte le corse delle multiple hanno 'multiple - 800m', quindi se c'è un numero potrebbe essere una corsa.
        if any(char.isdigit() for char in nome_evento):
            warning_evento = (warning_evento + ' ' + 'PM sus').strip()
        if 'thlon' in nome_evento:
            evento_generale = 'prove multiple'
            check = check + 1
        
    # siepi
        
    if check > 1:
        warning_evento = (warning_evento + ' ' + str(check) + ' if').strip()
    
    # Sperando non sia rimasto nulla (falso ho trascurato le siepi, ma siamo indoor per ora)
    if check == 0:
        evento_generale = 'boh'

    return evento_generale, warning_evento


def check_master(nome):
    
    ## Controlla se l'evento viene taggato come evento master, utilizzato nel caso non siano state trovate altre informazioni
    ## su altezze di ostacoli, massse di pesi/giavellotti/dischi/martelli, distanze
    ## restituisce True o False
    
    nome = nome.lower()
    
    match_master0 = re.search(r'master', nome)
    match_master1 = re.search(r's\d{2}', nome.replace(' ',''))
    match_master2 = re.search(r'm\d{2}', nome.replace(' ',''))     # M70+
    match_master3 = re.search(r'sm\d{2}', nome.replace(' ',''))    # SM45
    match_master4 = re.search(r'f\d{2}', nome.replace(' ',''))     # F90
    match_master5 = re.search(r'sf\d{2}', nome.replace(' ',''))    # SF50
    
    if match_master0 or match_master1 or match_master2 or match_master3 or match_master4 or match_master5:
        return True
    else:
        return False


def info_categoria(nome):
    
    ## funzione scritta per inferire la categoria di un evento.
    ## DA USARE CON ATTENZIONE
    ## TENERE L'INPUT ORIGINALE CON SPAZI TRA PAROLE
    ## il print di warning avviene solose trova due categorie diverse non senior/promesse
    ## restituisce E, R, CF, CM, AF, AM, JM, AF, AM oppure una stringa vuota
    ## genere per esordienti e ragazzi non è rilevanti.
    ## junior e promesse donne hanno le stesse gare delle assolute
    
    nome = nome.strip().lower()
    cat = ''
    check = 0
    
    # Assoluti
    match_hs_ass0 = re.search(r'\badulti u\b', nome)   # adulti u
    match_hs_ass1 = re.search(r'uomini', nome)         # uomini
    match_hs_ass2 = re.search(r'men', nome)            # men
    match_hs_ass3 = re.search(r'maschile', nome)       # maschile
    match_hs_ass4 = re.search(r'\bm\b', nome)          # m
    match_hs_ass5 = re.search(r'\bu\b', nome)          # u
    match_hs_ass6 = re.search(r'\bpromesse u\b', nome) # promesse u
    match_hs_ass7 = re.search(r'\bpromesse m\b', nome) # promesse m
    match_hs_ass8 = re.search(r'\bpm\b', nome)         # pm
    
    
    match_hs_ass9 = re.search(r'donne', nome)          # donne
    match_hs_ass10 = re.search(r'women', nome)         # women
    match_hs_ass11 = re.search(r'femminile', nome)     # femminile
    match_hs_ass12= re.search(r'\bf\b', nome)          # f
    match_hs_ass13 = re.search(r'\bd\b', nome)         # d
    match_hs_ass14 = re.search(r'\badulti d\b', nome)  # adulti d
    match_hs_ass15 = re.search(r'\bpromesse d\b', nome) # promesse d
    match_hs_ass16 = re.search(r'\bpromesse f\b', nome) # promesse f
    match_hs_ass17 = re.search(r'\bpf\b', nome)         # pf
    
    
    if match_hs_ass0 or match_hs_ass1 or match_hs_ass2 or match_hs_ass3 or match_hs_ass4 or match_hs_ass5 or match_hs_ass6 or match_hs_ass7 or match_hs_ass8:
        cat = 'SM'
    
    if match_hs_ass9 or match_hs_ass10 or match_hs_ass11 or match_hs_ass12 or match_hs_ass13 or match_hs_ass14 or match_hs_ass15 or match_hs_ass16 or match_hs_ass17:
        cat = 'SF'
    
    # Esordienti
    match_eso1 = re.search(r'\besordienti\b', nome) # esordienti
    match_eso2 = re.search(r'\bef\d+', nome)        # EF8
    match_eso3 = re.search(r'\bem\d+', nome)        # EM5
    match_eso4 = re.search(r'\bef\b', nome)         # EF
    match_eso5 = re.search(r'\bef\b', nome)         # EM
    
    if match_eso1 or match_eso2 or match_eso3 or match_eso4 or match_eso5:
        check = check + 1
        cat = 'E'

    # Ragazzi
    match_hs_r0 = re.search(r'\bragazz', nome)   # ragazz
    match_hs_r1 = re.search(r'\brm\b', nome)   # rm
    match_hs_r2 = re.search(r'\brf\b', nome)   # rf
    
    if match_hs_r0 or match_hs_r1 or match_hs_r2:
        cat = 'R'
        check = check + 1
    
    # Cadetti
    match_hs_c1 = re.search(r'cadetti', nome)  # cadetti
    match_hs_c2 = re.search(r'\bcm\b', nome)   # cm
    match_hs_c3 = re.search(r'cadette', nome)  # cadettte
    match_hs_c4 = re.search(r'\bcf\b', nome)   # cf
    
    if match_hs_c1 or match_hs_c2:
        cat = 'CM'
        check = check + 1

    if match_hs_c3 or match_hs_c4:
        cat = 'CF'
        check = check + 1
    
    # Allievi
    match_hs_a1 = re.search(r'allievi', nome)  # allievi
    match_hs_a2 = re.search(r'\bam\b', nome)   # am
    match_hs_a3 = re.search(r'allieve', nome)  # allieve
    match_hs_a4 = re.search(r'\baf\b', nome)   # af
    
    if match_hs_a1 or match_hs_a2:
        cat = 'AM'
        check = check + 1

    if match_hs_a3 or match_hs_a4:
        cat = 'AF'
        check = check + 1
    
    # Junior
    match_hs_j1 = re.search(r'junior u', nome)     # junior u
    match_hs_j2 = re.search(r'junior m', nome)     # junior m
    match_hs_j3 = re.search(r'juniores u', nome)   # juniores u
    match_hs_j4 = re.search(r'juniores m', nome)   # juniores m
    match_hs_j5 = re.search(r'\bjm\b', nome)       # jm
    match_hs_j6 = re.search(r'junior d', nome)     # junior d
    match_hs_j7 = re.search(r'junior f', nome)     # junior f
    match_hs_j8 = re.search(r'juniores d', nome)   # juniores d
    match_hs_j9 = re.search(r'juniores f', nome)   # juniores f
    match_hs_j10 = re.search(r'\bjf\b', nome)      # jf
    
    if match_hs_j1 or match_hs_j2 or match_hs_j3 or match_hs_j4 or match_hs_j5:
        cat = 'JF'
        check = check + 1
    
    if match_hs_j6 or match_hs_j7 or match_hs_j8 or match_hs_j9 or match_hs_j10:
        cat = 'JM'
        check = check + 1
    
    if check > 1:
        print('Ho trovato '+str(check)+'categorie diverse. Restituisco la più giovane. Non fidarti di me!')
    
    return cat


def info_ostacoli(nome):
    
    ## gli ostacoli sono così incasinati che ho dovuto fare una funzione a parte
    
    nome = nome.lower().replace('finale','').strip().replace('ostacoli', 'hs')
    spec = ''
    warn_spec = ''
    found = False

    #Se non comincia con un numero, faccio solo un guess su quale potrebbe essere la distanza della gara
    if nome[0].isdigit() is False:        
        match_dist = re.search(r'\d+', nome)
        if match_dist:
            dist = match_dist[0].strip()
            spec = dist+' Hs'
            warn_spec = 'Distanza a caso'
        else:
            spec = 'ostacoli'
            warn_spec = 'Non conosco la distanza'
        return spec, warn_spec

    # D'ora in poi possiamo assumere che 'nome' cominci con un numero

    # esordienti
    match_eso1 = re.search(r'esordienti', nome)     # esordienti
    match_eso2 = re.search(r'\bef\d+', nome)        # EF8
    match_eso3 = re.search(r'\bem\d+', nome)        # EM5
    match_eso4 = re.search(r'\bef\b', nome)         # EF
    match_eso5 = re.search(r'\bem\b', nome)         # EM
    match_eso6 = re.search(r'\bef\w\b', nome)       # EFA
    match_eso7 = re.search(r'\bem\w\b', nome)       # EMB
    
    if not(found) and (match_eso1 or match_eso2 or match_eso3 or match_eso4 or match_eso5 or match_eso6 or match_eso7):
        dist = re.search(r'\d+', nome)[0]
        spec = dist.strip()+' Hs Esordienti'
        found = True
    
    # master
    if not(found) and check_master(nome):
        spec = re.search(r'\d+', nome)[0].strip()+' Hs Master'
        found = True
        
    # togliamoci dai piedi quelli scritti bene
    pat_hs0 = r'\d+hsh\d+-\d.\d{2}' # 60hsh106-9.14
    match_hs0 = re.search(pat_hs0, nome.replace(' ',''))
    
    if not(found) and match_hs0:
        spec = match_hs0[0].strip().split('h')[0]+' Hs h'+match_hs0[0].strip().split('h')[2][:-5]
        found = True
    
    # passiamo a quelli scritti senza distanza
    match_hs1 = re.search(r'h\d+', nome.replace(' ',''))      # h100
    
    if not(found) and match_hs1:
        print(nome)
        dist = re.search(r'\d+[^\d]*hs', nome.replace(' ', ''), re.IGNORECASE)[0]  # match full "number-junk-hs"
        dist = re.search(r'\d+', dist)[0]  # extract only the number
        h = match_hs1[0].strip().split('h')[-1]
        
        if int(dist) > 110:
            print('Benvenuto alle outdoor')
            
        spec = dist+' Hs h'+h
        found = True
    
    # ora devo indentificare le categorie se voglio sapere l'altezza dell'ostacolo
    # ragazzi
    dist = re.search(r'\d+', nome)[0].strip()
    match_hs_r0 = re.search(r'ragazz', nome)   # ragazz
    match_hs_r1 = re.search(r'\brm\b', nome)   # rm
    match_hs_r2 = re.search(r'\brf\b', nome)   # rf
    
    if not(found) and (match_hs_r0 or match_hs_r1 or match_hs_r2):
        spec = dist+' Hs h60'
        found = True
        
    # cadetti e cadette
    match_hs_c1 = re.search(r'cadetti', nome)  # cadetti
    match_hs_c2 = re.search(r'\bcm\b', nome)   # cm
    match_hs_c3 = re.search(r'cadette', nome)  # cadettte
    match_hs_c4 = re.search(r'\bcf\b', nome)   # cf
    
    if not(found) and (match_hs_c1 or match_hs_c2):
        spec = dist+' Hs h84'
        found = True

    if not(found) and (match_hs_c3 or match_hs_c4):
        spec = dist+' Hs h76'
        found = True
        
    # allievi e allieve
    match_hs_a1 = re.search(r'allievi', nome)  # allievi
    match_hs_a2 = re.search(r'\bam\b', nome)   # am
    match_hs_a3 = re.search(r'allieve', nome)  # allieve
    match_hs_a4 = re.search(r'\baf\b', nome)   # af
    
    if not(found) and (match_hs_a1 or match_hs_a2):
        spec = dist+' Hs h91'
        found = True

    if not(found) and (match_hs_a3 or match_hs_a4):
        spec = dist+' Hs h76'
        found = True
        
    # junior
    match_hs_j1 = re.search(r'junior u', nome)     # junior u
    match_hs_j2 = re.search(r'junior m', nome)     # junior m
    match_hs_j3 = re.search(r'juniores u', nome)   # juniores u
    match_hs_j4 = re.search(r'juniores m', nome)   # juniores m
    match_hs_j5 = re.search(r'\bjm\b', nome)       # jm
    match_hs_j6 = re.search(r'junior d', nome)     # junior d
    match_hs_j7 = re.search(r'junior f', nome)     # junior f
    match_hs_j8 = re.search(r'juniores d', nome)   # juniores d
    match_hs_j9 = re.search(r'juniores f', nome)   # juniores f
    match_hs_j10 = re.search(r'\bjf\b', nome)      # jf
    
    if not(found) and (match_hs_j1 or match_hs_j2 or match_hs_j3 or match_hs_j4 or match_hs_j5):
        spec = dist+' Hs h100'
        found = True
    
    if not(found) and (match_hs_j6 or match_hs_j7 or match_hs_j8 or match_hs_j9 or match_hs_j10):
        spec = dist+' Hs h84'
        found = True
    
    # In teoria mi sono rimasti solo gli assoluti ora. Devo solo distinguere tra uomo e donna
    match_hs_ass1 = re.search(r'uomini', nome)     # uomini
    match_hs_ass2 = re.search(r'men', nome)        # men
    match_hs_ass3 = re.search(r'maschil\w', nome)   # maschile
    match_hs_ass4 = re.search(r'\bm\b', nome)      # m
    match_hs_ass5 = re.search(r'\bu\b', nome)      # u
    match_hs_ass6 = re.search(r'donne', nome)      # donne
    match_hs_ass7 = re.search(r'women', nome)      # women
    match_hs_ass8 = re.search(r'femminil\w', nome)  # maschile
    match_hs_ass9 = re.search(r'\bf\b', nome)      # f
    match_hs_ass10 = re.search(r'\bd\b', nome)     # d
    
    if not(found) and (match_hs_ass1 or match_hs_ass2 or match_hs_ass3 or match_hs_ass4 or match_hs_ass5):
        spec = dist+' Hs h106'
        warn_spec = 'a esclusione'
        found = True
    
    if not(found) and (match_hs_ass6 or match_hs_ass7 or match_hs_ass8 or match_hs_ass9 or match_hs_ass10):
        spec = dist+' Hs h84'
        warn_spec = 'a esclusione'
        found = True
    
    if not(found):
        spec = dist+' Hs'
        warn_spec = 'non conosco l\'altezza'
    
    #if check > 2:
    #    warn_spec = 'sus, ho trovato '+str(check)+' pattern'
        
    return spec, warn_spec
        

def assegna_evento_specifico(nome, eve):
    
    ## dato il nome dell'evento che compare nella pagina della gara e la categoria generale che gli è stata assegnata da
    ## asssegna_evento_generale() analizza il nome dell'evento per ottenere maggiori informazioni (altezza ostacoli, categoria
    ## massa di peso, disco, martello e giavellotto)
    ## usa info_ostacoli() e check_master()
    ## ritorna l'evento specifico e una stringa con possibli warning
    
    check = 0 # serve a controllare se per motivi scemi l'evento specifico viene scritto due volte
    nome = nome.lower().replace('finale','').replace('finale','').strip()
    nome = nome.replace('1\u00b0','').replace('2\u00b0','').replace('3\u00b0','')
    spec = '' # evento specifico
    warn_spec = ''
    
    # qualche utile pattern per i master
   
    
    # SALTI e VORTEX, evento generale: ['asta','lungo da fermo','lungo','alto','triplo','quadruplo']
    if eve == 'altro':              spec = 'altro'
    elif eve == 'asta':             spec = 'Salto con l\'asta'
    elif eve == 'lungo da fermo':   spec = 'Salto in lungo da fermo'
    elif eve == 'lungo':            spec = 'Salto in lungo'
    elif eve == 'alto':             spec = 'Salto in alto'
    elif eve == 'triplo':           spec = 'Salto triplo'
    elif eve == 'quadruplo':        spec = 'Salto quadruplo'
    elif eve == 'vortex':           spec = 'Vortex'
    
    # CORSE
    
    elif re.search(r'1miglio', nome.replace(' ','')): spec = '1 Miglio' # evviva le freedom units
    elif re.search(r'2miglia', nome.replace(' ','')): spec = '2 Miglia'
    elif eve == 'corsa piana':
        nome = nome.replace(' ','')
        
        pat_corse = r'^\d+' # assumo che le corse abbiano sempre la distanza all'inizio
        match_corse = re.search(pat_corse, nome)
        
        if match_corse:
            spec = match_corse[0].strip() + 'm'

    # STAFFETTA
    
    elif eve == 'staffetta':
        nome = nome.replace(' ','')
        
        
        if 'giro' in nome:
            nome = nome.replace('1giro','200').replace('1 giro','200')
        if 'giri' in nome:
            nome = nome.replace('2giri','400').replace('2 giri','400')

        pat_staff1 = r'\d+x\d+'    # 4x100
        pat_staff2 = r'\d+ x \d+'  # 4 x 100
        match_staff1 = re.search(pat_staff1, nome)
        match_staff2 = re.search(pat_staff2, nome)
            
        if match_staff1:
            spec = match_staff1[0].strip() + 'm'
            check = check + 1
        
        if match_staff2:
            spec = match_staff2[0].strip().replace(' ','') + 'm'     
            check = check + 1
        
        if check == 0:
            spec = 'staffetta'
            warn_spec = 'non conosco la staffetta'
        
        if check == 2:
            warn_spec = 'sus, ho trovato entrambi i pattern'   
    
    # MARCIA
    
    elif eve == 'marcia':
        nome = nome.replace(' ','')
        
        match_marcia1 = re.search(r'\d+m', nome)   # 3000m
        match_marcia2 = re.search(r'\d+km', nome)  # 3km
        match_marcia3 = re.search(r'km\d+', nome)  # km3
        
        if match_marcia1:
            spec = 'Marcia '+match_marcia1[0].strip()
            check = check + 1
        
        if match_marcia2:
            spec = 'Marcia '+match_marcia2[0][:-2].strip()+'000m'
            check = check + 1
        
        if match_marcia3:
            spec = 'Marcia '+match_marcia3[0][2:].strip()+'000m'
            check = check + 1
            
        if check == 0:
            if check_master(nome):
                spec = 'Marcia Master'
            else:
                spec = 'Marcia'
                warn_spec = 'non conosco la distanza'
        
        if check == 2:
            warn_spec = 'sus, ho trovato entrambi i pattern'
    
    # DISCO
    
    elif eve == 'disco':
        nome = nome.replace(' ','')
        
        
        pat_disco = r'kg\d+.\d+'
        match_disco = re.search(pat_disco, nome)
        
        if match_disco:
            spec = 'Disco '+match_disco[0][2:].strip()+'Kg'
            check = check + 1
        
        if check == 0:
            if check_master(nome):
                spec = 'Disco Master'
            else:
                spec = 'Disco'
                warn_spec = 'non conosco la massa'
            
    # GIAVELLOTTO
    
    elif eve == 'giavellotto':
        nome = nome.replace(' ','')
        
        pat_giav1 = r'g\d+'     # g400
        pat_giav2 = r'gr\d+'    # gr400
        pat_giav3 = r'\d+g'     # 400g
        match_giav1 = re.search(pat_giav1, nome)
        match_giav2 = re.search(pat_giav2, nome)        
        match_giav3 = re.search(pat_giav3, nome)
        
        if match_giav1:
            spec = 'Giavellotto '+match_giav1[0][1:].strip()+'g'
            check = check + 1

        if match_giav2:
            spec = 'Giavellotto '+match_giav2[0][2:].strip()+'g'
            check = check + 1
            
        if match_giav3:
            spec = 'Giavellotto '+match_giav3[0][:-1].strip()+'g'
            check = check + 1
        
        if check == 0:
            if check_master(nome):
                spec = 'Giavellotto Master'
            else:
                spec = 'Giavellotto'
                warn_spec = 'non conosco la massa'
        
        if check > 1:
            warn_spec = 'sus, ho trovato tanti i pattern'
            
    # MARTELLO
    
    elif eve == 'martello':
        nome = nome.replace(' ','')
        
        pat_mart = r'kg\d+.\d+'
        match_mart = re.search(pat_mart, nome)
        
        if match_mart:
            spec = 'Martello '+match_mart[0][2:].strip()+'Kg'
            check = check + 1
        
        if check == 0:
            if check_master(nome):
                spec = 'Martello Master'
            else:
                spec = 'Martello'
                warn_spec = 'non conosco la massa'
            
    # PESO
    # Da migliorare, ma ancora non sono sicuro di come fare e non ho voglia di pensarci
    
    elif eve == 'peso':
        nome = nome.replace(' ','')
        
        match_peso1 = re.search(r'kg\d+', nome)        # kg5               Nota: l'ordine è importante perchè
        match_peso2 = re.search(r'kg\d+.\d+', nome)    # kg5.000           la stringa 1 e 3 si può anche trovare
        match_peso3 = re.search(r'\d+kg', nome)        # 5kg               nella stringa 2 e 4. Quindi i match a
        match_peso4 = re.search(r'\d+.\d+kg', nome)    # 5.000kg           2 e 4 devono andare dopo per sovraiscrivere
        
        
        if match_peso1:
            spec = 'Peso '+match_peso1[0][2:].strip()+'.000Kg'
            check = check + 1
            
        if match_peso2:
            spec = 'Peso '+match_peso2[0][2:].strip()+'Kg'
            check = check + 1

        if match_peso3:
            spec = 'Peso '+match_peso3[0][:-2].strip()+'.000Kg'
            check = check + 1
        
        if match_peso4:
            spec = 'Peso '+match_peso4[0][:-2].strip()+'Kg'
            check = check + 1
        if '7' in spec: spec = 'Peso 7.260Kg'
        if check == 0:
            if check_master(nome):
                spec = 'Peso Master'
            else:
                spec = 'Peso'
                warn_spec = 'non conosco la massa'
        
        if check > 2: # dovrebbe essere >1, ma guarda la nota sopra
            warn_spec = 'sus, ho trovato tanti i pattern'
    
    # OSTACOLI
    elif eve == 'ostacoli':
        (spec, warn_spec) = info_ostacoli(nome)
                
    else: spec = eve
    
    return spec, warn_spec
