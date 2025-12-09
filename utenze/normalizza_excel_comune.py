#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2023
# Roberto Marzocchi

'''
Lo script si occupa di normalizzare i file provenienti dai comuni del genovesato 

Fatto a partire dai dati inviati il 19/11/2025 da Torriglia

'''

#from msilib import type_short
import os, sys, re  # ,shutil,glob
import inspect

#import getopt  # per gestire gli input

#import pymssql

from datetime import date, datetime, timedelta


import xlsxwriter

import psycopg2

import cx_Oracle

currentdir = os.path.dirname(os.path.realpath(__file__))
parentdir = os.path.dirname(currentdir)
sys.path.append(parentdir)
from credenziali import *


#import requests

import logging

filename = inspect.getframeinfo(inspect.currentframe()).filename
path=os.path.dirname(sys.argv[0]) 
path1 = os.path.dirname(os.path.dirname(os.path.abspath(filename)))
nome=os.path.basename(__file__).replace('.py','')
#tmpfolder=tempfile.gettempdir() # get the current temporary directory
logfile='{0}/log/{1}.log'.format(path,nome)
errorfile='{0}/log/error_{1}.log'.format(path,nome)
#if os.path.exists(logfile):
#    os.remove(logfile)






# Create a custom logger
logging.basicConfig(
    level=logging.DEBUG,
    handlers=[
    ]
)

logger = logging.getLogger()

# Create handlers
c_handler = logging.FileHandler(filename=errorfile, encoding='utf-8', mode='w')
f_handler = logging.StreamHandler()
#f_handler = logging.FileHandler(filename=logfile, encoding='utf-8', mode='w')


c_handler.setLevel(logging.ERROR)
f_handler.setLevel(logging.DEBUG)


# Add handlers to the logger
logger.addHandler(c_handler)
logger.addHandler(f_handler)


cc_format = logging.Formatter('%(asctime)s\t%(levelname)s\t%(message)s')

c_handler.setFormatter(cc_format)
f_handler.setFormatter(cc_format)


# libreria per invio mail
import email, smtplib, ssl
import mimetypes
from email.mime.multipart import MIMEMultipart
from email import encoders
from email.message import Message
from email.mime.audio import MIMEAudio
from email.mime.base import MIMEBase
from email.mime.image import MIMEImage
from email.mime.text import MIMEText
from invio_messaggio import *

# libreria per scrivere file csv
import csv


import pandas
    



import re

def parse_address_old(addr_raw):

    ''' 
    Funzione per parsare l'indirizzo usando prevalentemente regexp 
    '''
    # Initialize output
    street, num, letter, color, scala, piano, interno = [None]*7

    if not isinstance(addr_raw, str):
        return street, num, letter, color, scala, piano, interno

    # Normalize input
    addr = addr_raw.strip().lower()

    # Rimuove "localita'" -> "località"
    #addr = addr.replace("localita'", "località")  
    
    
    # Rimuove "n.", "n", "num.", "num"
    #addr = re.sub(r'\b(n\.?|num\.?)\b', '', addr)


    

    
    # snc
    addr=addr.replace('/snc', '').strip()
    # piano terra in diversi formati
    addr=addr.replace('p. t', '').strip()
    addr=addr.replace('p. pt', '').strip()
    addr=addr.replace('pt', '').strip()
    #addr = re.sub(r'\s+', ' ', addr).strip()


    """
    questa parte fa casino
    # --- 0. Gestione formato "12/CA" (prima del civico standard)
    m = re.search(r'\b(\d+)\s*/\s*([a-z]{1,3})\b', addr)
    if m:
        num = m.group(1)
        letter = m.group(2).upper()
        addr = addr.replace(m.group(0), '')
    """


    # --- 1. numero -----------------------------------
    # !!! PROBLEMA NDL CASO DI LETTERE 12/A 
    m = re.search(r'\b(n\.)\s*([a-z0-9]+)\b', addr)
    if m:
        num = m.group(2).upper()
        addr = addr.replace(m.group(0), '')

    
    
    # --- 2. piano (lo facio prima della scala perchè ci sono un po' di p. S p.S1 che non so cosa siano) ------------------------------------
    m = re.search(r'\b(p\.?|piano)\s*([a-z])\b', addr)
    if m:
        print(addr)
        #print(m) 
        piano = m.group(2).upper()
        addr = addr.replace(m.group(0), '')
        #print(piano)
        #print(addr)
        #exit()
        
        
    # --- 3. scala ------------------------------------
    m = re.search(r'\b(s\.?|scala|sc\.)\s*([a-z])\b', addr)
    if m:
        scala = m.group(2).upper()
        addr = addr.replace(m.group(0), '')

   


    # --- 4. interno -----------------------------------
    m = re.search(r'\b(int\.?|i\.?|interno)\s*([a-z0-9]+)\b', addr)
    if m:
        interno = m.group(2).upper()
        addr = addr.replace(m.group(0), '')


    # ----------------------------------------------------------
    # ⭐ 5) CIVICO + LETTERA + COLORE (BLOCCO FIXATO)
    #    Questa regex NON matcherà "via IV novembre"
    # ----------------------------------------------------------
    #
    # (?<![a-z]) → evita falsi positivi tipo "iv" o "viii"
    # (\d+)      → vero numero civico
    # ([a-z])?   → opzionale LETTERA singola (non "iv", "ter"...)
    # (r|rosso|n|nero)? → colore civico
    #
    m = re.search(
        r'(?<![a-z])\b(\d+)(?:\s*/\s*([a-z]))?(?:\s*(?:r|rosso|n|nero))?\b',
        addr
    )

    if m and num is None:  # usalo solo se non hai già catturato il civico da "n."
        num = m.group(1)
        if m.group(2):
            letter = m.group(2).upper()
        if m.group(3):
            color = "R" if m.group(3).startswith("r") else "N"
        addr = addr.replace(m.group(0), '')



    # a questo punto faccio un po' di pulizia finale
    
    # rimosso spazio punto 
    addr=addr.replace(' .', '').strip()
    #rimosso punto finale
    addr = re.sub(r'\.$', '', addr).strip()

    # --- 4. Street (tutto ciò che resta) ---------------
    street = addr.strip().rstrip(',')

    # Pulizia finale
    if letter == "" or (letter == "N" and color is None):
        letter = None
    if interno == "T":
        interno = None
        
        
    

    return street, num, letter, color, scala, piano, interno



def parse_address(addr_raw):

    ''' 
    Funzione per parsare l'indirizzo usando prevalentemente regexp 
    '''
    # Initialize output
    street, num, letter, color, scala, piano, interno = [None]*7

    if not isinstance(addr_raw, str):
        return street, num, letter, color, scala, piano, interno

    # Normalize input
    addr = addr_raw.strip().lower()

    # snc
    addr = addr.replace('/snc', '').strip()

    # piano terra in diversi formati
    addr = addr.replace('p. t', '').strip()
    addr = addr.replace('p. pt', '').strip()
    addr = addr.replace('pt', '').strip()

    # ---------------------------------------------------
    # 1. numero con formato "n. 12" o "n.12"
    # ---------------------------------------------------
    m = re.search(r'\b(n\.)\s*([a-z0-9]+)\b', addr)
    if m:
        num = m.group(2).upper()
        addr = addr.replace(m.group(0), '')

    # ---------------------------------------------------
    # 2. piano (p. a, piano a)
    # ---------------------------------------------------
    m = re.search(r'\b(p\.?|piano)\s*([a-z])\b', addr)
    if m:
        piano = m.group(2).upper()
        addr = addr.replace(m.group(0), '')

    # ---------------------------------------------------
    # 3. scala (s., scala, sc.)
    # ---------------------------------------------------
    m = re.search(r'\b(s\.?|scala|sc\.)\s*([a-z])\b', addr)
    if m:
        scala = m.group(2).upper()
        addr = addr.replace(m.group(0), '')

    # ---------------------------------------------------
    # 4. interno (int., interno, i.)
    # ---------------------------------------------------
    m = re.search(r'\b(int\.?|i\.?|interno)\s*([a-z0-9]+)\b', addr)
    if m:
        interno = m.group(2).upper()
        addr = addr.replace(m.group(0), '')


    # ----------------------------------------------------------
    # 5. CIVICO + LETTERA (FIXATO) + COLORE
    #
    # FIX: NON include più lo slash nel match → evita "/b" residui
    #
    # (\d+)              → numero
    # (?: / lettera )?   → cattura la lettera DOPO slash, ma NON lo slash
    # (r|rosso|n|nero)?  → colore civico
    # ----------------------------------------------------------
    m = re.search(
        r'(?<![a-z])\b(\d+)(?:\s*/\s*([a-z]))?(?:\s*(r|rosso|n|nero))?\b',
        addr
    )

    if m and num is None:  # usalo solo se non hai già preso "n. X"
        num = m.group(1)

        if m.group(2):
            letter = m.group(2).upper()

        if m.group(3):
            color = "R" if m.group(3).startswith("r") else "N"

        # Rimuove SOLO la parte trovata (senza lasciare slash)
        addr = addr.replace(m.group(0), '')


    # ---------------------------------------------------
    # PULIZIA FINALE
    # ---------------------------------------------------

    # rimosso spazio punto 
    addr = addr.replace(' .', '').strip()

    # rimosso punto finale
    addr = re.sub(r'\.$', '', addr).strip()

    # street = tutto ciò che resta
    street = addr.strip().rstrip(',')

    # pulizia campi
    if letter == "" or (letter == "N" and color is None):
        letter = None

    if interno == "T":  # come ti serviva
        interno = None

    return street, num, letter, color, scala, piano, interno



#parsed = df.iloc[:,0].apply(parse_address)
#out = pd.DataFrame(parsed.tolist(), columns=['via','civico','lettera','colore','scala','interno'])


  

def main():
    
    # TO DO ricerca file dentro una cartella
    
    comune='TO' # torriglia, id=20

    data_file='20251119'
    
    excel_data_df = pandas.read_excel("{}/input/Estrazione_utenze_torriglia.xlsx".format(path), sheet_name='UTENZE')



    #logger.debug(excel_data_df)
    logger.info('Leggo il file excel con la libreria Pandas')
    logger.debug(excel_data_df.columns.ravel())
    try:
        denominazioni=excel_data_df['denominazione'].tolist()
        logger.debug(f'Lunghezza denominazioni {len(denominazioni)}')
    except Exception as e:
        logger.error(e)
    
    
    try:
        cognome_ragsoc=excel_data_df['cognome/ragione sociale'].tolist()
        logger.debug(f'Lunghezza cognome_ragsoc {len(cognome_ragsoc)}')
    except Exception as e:
        logger.error(e)
    
    try:
        nome=excel_data_df['nome'].tolist()
        logger.debug(f'Lunghezza nome {len(nome)}')
    except Exception as e:
        logger.error(e)
        
    try:
        indirizzo=excel_data_df['indirizzo immobile'].tolist()
        indirizzi= excel_data_df['indirizzo immobile'] # senza tolist perchè poi devo applicare la funzione definita prima
        logger.debug(f'Lunghezza indirizzo immobile {len(indirizzo)}')
    except Exception as e:
        logger.error(e)  
        
    try:
        cf_piva=excel_data_df['codice fiscale / partita IVA'].tolist()
        logger.debug(f'Lunghezza cf_piva {len(cf_piva)}')
    except Exception as e:
        logger.error(e)     
    
    
    
    # controllo che non ci siano CF / pive nulli
    i=0 
    while i< len(cf_piva):
        if cf_piva[i]==None:
            logger.error('''CF dell'utente {} null'''.format(denominazioni[i]))
            exit()
        i+=1
    #exit()
    
    
    try:
        data_cessazione=excel_data_df['data cessazione'].tolist()
        logger.debug(len(cognome_ragsoc))
    except Exception as e:
        logger.error(e)
    
    
    parsed = indirizzi.apply(parse_address)

    parsed_df = pandas.DataFrame(parsed.tolist(),
                         columns=['via', 'civico', 'lettera', 'colore', 'scala', 'piano', 'interno'])
    
    
    lista_vie = parsed_df['via'].tolist()
    lista_civici = parsed_df['civico'].tolist()
    lista_lettere = parsed_df['lettera'].tolist()
    lista_colore = parsed_df['colore'].tolist()
    lista_scala = parsed_df['scala'].tolist()
    lista_piano = parsed_df['piano'].tolist()
    lista_interni = parsed_df['interno'].tolist()
    #exit()
    k=0
    while k<len(lista_vie):
        if re.match('via magioncalda', lista_vie[k]):
            logger.debug(f'''riga = {k+2}, via = {lista_vie[k]}, Civ = {lista_civici[k]} Int = {lista_interni[k]}, 
                     Scala = {lista_scala[k]},  P. {lista_piano[k]}, Lett = {lista_lettere[k]}, Colore={lista_colore[k]}''')
        k+=1
    
    
    #exit()
    
    # a questo punto bisogna scrivere in una tabella temporanea di un DB e recuperare le informazioni sulle vie con una funzione di similitudine
    
    

    """
    # modifiche a liste
    via=[]
    civ=[]
    piano=[]
    scala=[]
    interno=[]
    i=0
    while i<len(indirizzo):
        #logger.debug(indirizzo[i])
        
        if ' n.' in indirizzo[i]:
            via.append(indirizzo[i].split(' n.')[0].strip())
            
            
            
            if ' i.' in indirizzo[i]:
                civ.append(indirizzo[i].split(' n.')[1].split(' i.')[0].strip())
                interno.append(indirizzo[i].split(' n.')[1].split(' i.')[1].strip())
            else:
                civ.append(indirizzo[i].split(' n.')[1].strip())
                interno.append(None)
        else:
            via.append(indirizzo[i].strip())
            civ.append(None)
            interno.append(None)
        
        
        
        i+=1
    
    logger.debug(len(indirizzo))
    logger.debug(len(via))
    logger.debug(len(civ))
    logger.debug(len(interno))
    
    
    
    num_civ=[]
    let_civ=[]
    i=0
    while i<len(indirizzo):
        #logger.debug('{} - {}'.format(indirizzo[i], civ[i]))
        if civ[i]== None:
            num_civ.append(None)
            let_civ.append(None) 
        else:
            list_civ=list(filter(None, re.split(r'(\d+)', civ[i].strip())))
            #logger.debug(list_civ)
            if len(list_civ)==1:
                num_civ.append(civ[i])
                let_civ.append(None)
            elif len(list_civ)==2:
                num_civ.append(list_civ[0])
                if len(list_civ[1].replace('/','').strip())==1:
                    let_civ.append(list_civ[1].replace('/','').strip())
                else:
                    let_civ.append(None)
                #let_civ.append(list_civ[1].replace('/','').replace('p. T','').replace('p. P.','').replace('PT','').replace('PS','').replace('s. D','').replace('s. S','').strip())
            else:
                logger.warning('{0} Non riesco a separare il civico {1} - civ {2}'.format(i, indirizzo[i], civ[i]))
                num_civ.append(civ[0])
                let_civ.append(None)
        i+=1
            
    logger.debug(len(num_civ))
    logger.debug(len(let_civ))
    
    logger.debug('Stampo gli indirizzi')
    logger.debug(indirizzo)
    exit()
    """
    
    
    
    
    """
    # ricavo il nome 
    
    nome=[]
    i=0
    while i<len(denominazioni):
        nome.append(denominazioni[i].replace(cognome_ragsoc[i],'').strip())
        i+=1
    """
    
    
    via_unica=list(dict.fromkeys(lista_vie))
    
    logger.debug(via_unica)
    logger.debug(f'Di {len(lista_vie)} righe del file excel ho trovato {len(via_unica)} vie univoche')
    exit()
                   
    logger.debug(nome[1])  
    
    
    # Mi connetto al DB oracle
    cx_Oracle.init_oracle_client(percorso_oracle) # necessario configurare il client oracle correttamente
    #cx_Oracle.init_oracle_client() # necessario configurare il client oracle correttamente
    parametri_con='{}/{}@//{}:{}/{}'.format(user_strade,pwd_strade, host_uo,port_uo,service_uo)
    logger.debug(parametri_con)
    con = cx_Oracle.connect(parametri_con)
    logger.info("Collegato a DB Oracle. Versione ORACLE: {}".format(con.version))
    cur = con.cursor()
    cur2 = con.cursor()
    
    
    query_select= '''SELECT count(*) FROM STRADE.UTENZE_FUORI_GENOVA ufg 
JOIN STRADE.COMUNI c ON c.ID_COMUNE = ufg.ID_COMUNE 
WHERE c.PREFISSO_UTENTI = :c1 '''
    try:
        cur.execute(query_select, (comune,))
        risultato=cur.fetchall()
    except Exception as e:
        logger.error(query_select)
        logger.error(e)
        
    for tt in risultato:
        tp=tt[0] #totale precedente su DB
    cur.close()
    cur = con.cursor()
    
    
    codice_via_unica=[]
    # cerco le vie non presenti su DB
    i=0
    while i< len(via_unica):
        query_select= '''SELECT s.CODICE_VIA, s.NOME1, s.NOME2, s.NOME_BREVE 
                    from STRADE.STRADE s
                    JOIN STRADE.COMUNI c ON c.ID_COMUNE = s.COMUNE  
                    WHERE (trim(REPLACE(s.NOME2, concat(concat('(',:c1),')'))) LIKE upper(:c2) 
                    OR
                    trim(REPLACE(s.NOME_BREVE, concat(concat('(',:c1),')'))) LIKE upper(:c2)
                    ) and c.PREFISSO_UTENTI = :c1'''
        try:
            cur.execute(query_select, (comune,via_unica[i],))
            risultato=cur.fetchall()
            #macro_tappe.append(tappa[2])
        except Exception as e:
            check_error=2
            logger.error('''{}{}'''.format((comune,via_unica[i])))
            logger.error(query_select)
            logger.error(e)
        if len(risultato)==0:
            logger.error('Non trovo {}'.format(via_unica[i]))
            codice_via_unica.append(None)
        elif len(risultato)==1:
            for cv in risultato:
                # faccio UPDATE di ANAGR_SER_PER_UO
                codice_via_unica.append(cv[0]) 
        else:
            logger.error('{} trovata due volte'.format(via_unica[i]))
            
        i+=1
    
    cur.close()
    cur = con.cursor()
    
    # check se c_handller contiene almeno una riga 
    check=error_log_mail(errorfile, 'roberto.marzocchi@amiu.genova.it', os.path.basename(__file__), logger)
    
    if check==200:
        logger.error('Non trovo corrispondenza con alcune vie, per cui è stta inviata una mail ad assterritorio per sistemare le incongruenze')
        exit()
    else :
        logger.debug('Nessun errore prosso procedere')
        logger.debug(codice_via_unica)
        logger.debug(len(codice_via_unica))
    
    
    logger.debug('Per ora mi fermo qua')    
    exit()
    
    
    
    nome_file="utenze_{0}_variazioni.xlsx".format(comune)
    file_variazioni="{0}/utenze/output/{1}".format(path,nome_file)
    
    
    workbook = xlsxwriter.Workbook(file_variazioni)
    
    title = workbook.add_format({'bold': True, 'bg_color': '#F9FF33', 'valign': 'vcenter', 'center_across': True,'text_wrap': True})
    text = workbook.add_format({'text_wrap': True})
    #text_green = workbook.add_format({'text_wrap': True, 'bg_color': '#ccffee'})
    #date_format = workbook.add_format({'num_format': 'dd/mm/yyyy hh:mm', 'bg_color': '#ccffee'})
    #text_dispari= workbook.add_format({'text_wrap': True, 'bg_color': '#ffcc99'})
    #date_format_dispari = workbook.add_format({'num_format': 'dd/mm/yyyy hh:mm', 'bg_color': '#ffcc99'})

    
    
    
    w0 = workbook.add_worksheet('OK')
    w1 = workbook.add_worksheet('Da cessare')
    #w2 = workbook.add_worksheet('Da controllare')
    w3 = workbook.add_worksheet('Da aggiungere')

    w0.write(0, 0, 'cf_piva', title) 
    w0.write(0, 1, 'cognome_ragsoc', title) 
    w0.write(0, 2, 'nome', title)
    w0.write(0, 3, 'id_via', title)
    w0.write(0, 4, 'via', title)
    w0.write(0, 5, 'num_civ', title) 
    w0.write(0, 6, 'let_civ', title)
    w0.write(0, 7, 'int', title)
    
    
    w1.write(0, 0, 'cf_piva', title) 
    w1.write(0, 1, 'cognome_ragsoc', title) 
    w1.write(0, 2, 'nome', title)
    w1.write(0, 3, 'id_via', title)
    w1.write(0, 4, 'via', title)
    w1.write(0, 5, 'num_civ', title) 
    w1.write(0, 6, 'let_civ', title)
    w1.write(0, 7, 'int', title)
    w1.write(0, 8, 'data_cessazione', title)
     
    '''w2.write(0, 0, 'cf_piva', title) 
    w2.write(0, 1, 'cognome_ragsoc', title) 
    w2.write(0, 2, 'nome', title)
    w2.write(0, 3, 'id_via', title)
    w2.write(0, 4, 'via', title)
    w2.write(0, 5, 'num_civ', title) 
    w2.write(0, 6, 'let_civ', title)
    w2.write(0, 7, 'int', title)
    w2.write(0, 8, 'data_cessazione', title) '''
    
    
    w3.write(0, 0, 'cf_piva', title) 
    w3.write(0, 1, 'cognome_ragsoc', title) 
    w3.write(0, 2, 'nome', title)
    w3.write(0, 3, 'id_via', title)
    w3.write(0, 4, 'via', title)
    w3.write(0, 5, 'num_civ', title) 
    w3.write(0, 6, 'let_civ', title)
    w3.write(0, 7, 'int', title)
    w3.write(0, 8, 'data_cessazione', title)
    w3.write(0, 9, 'dati_simili_presenti', title) 
    
    # larghezza colonne
    
    w0.set_column(0, 0, 20)
    w0.set_column(1, 2, 25)
    w0.set_column(3, 3, 10)
    w0.set_column(4, 4, 25)
    w0.set_column(5, 7, 10)
    #w0.set_column(8, 8, 20)
    
    
    w1.set_column(0, 0, 20)
    w1.set_column(1, 2, 25)
    w1.set_column(3, 3, 10)
    w1.set_column(4, 4, 25)
    w1.set_column(5, 7, 10)
    w1.set_column(8, 8, 20)
    
    '''w2.set_column(0, 0, 20)
    w2.set_column(1, 2, 25)
    w2.set_column(3, 3, 10)
    w2.set_column(4, 4, 25)
    w2.set_column(5, 7, 10)
    w2.set_column(8, 8, 20)
    '''
    
    w3.set_column(0, 0, 20)
    w3.set_column(1, 2, 25)
    w3.set_column(3, 3, 10)
    w3.set_column(4, 4, 25)
    w3.set_column(5, 7, 10)
    w3.set_column(8, 8, 20)
    
    campo_controllo=[]
    # cerco le vie non presenti su DB
    i=0
    i0=1
    i1=1
    i2=1
    i3=1
    nt = 0 # non trovati
    while i < len(cf_piva):
        #logger.debug(i)
        if str(cf_piva[i]).strip()==None:
            logger.error('''CF dell'utente {} null'''.format(denominazioni[i]))
            exit()
        if interno[i] is None:
            interno_temp='0'
        else :
            interno_temp=interno[i]
        if num_civ[i] is None:
            num_civ_temp='0'
        else :
            num_civ_temp=num_civ[i]
        codice=codice_via_unica[via_unica.index(via[i])]
        #logger.debug(str(cf_piva[i]))
        #logger.debug('''{}'''.format(interno[i])) # cf_piva[i], codice,num_civ[i], let_civ[i],interno[i])))
        # cerco le corrispondenze
        query_select1= '''SELECT ufg.* FROM STRADE.UTENZE_FUORI_GENOVA ufg 
                JOIN STRADE.COMUNI c ON c.ID_COMUNE = ufg.ID_COMUNE 
                WHERE c.PREFISSO_UTENTI = :c1 
                AND (trim(CF_PIVA) = :c2 
                OR 
                (trim(COGNOME) LIKE :c3 and trim(NOME) LIKE :c4))
                AND CODICE_VIA = :c5 
                AND trim(CIVICO) = :c6
                AND INTERNO = LPAD(NVL(:c7,0), 3, '0')'''
        try:
            if let_civ[i] is None or let_civ[i].strip()=='':
                cur.execute(query_select1, (comune,str(cf_piva[i]).strip(),str(cognome_ragsoc[i]).strip(), str(nome[i]).strip(),int(codice),num_civ_temp,interno_temp))
            else:
                query_select1 = '{} and upper(LETTERA_CIVICO) = upper(:c8)'.format(query_select1)
                cur.execute(query_select1, (comune,str(cf_piva[i]).strip(),str(cognome_ragsoc[i]).strip(), str(nome[i]).strip(),int(codice),num_civ_temp,interno_temp,let_civ[i]))
            risultato=cur.fetchall()
            #macro_tappe.append(tappa[2])
        except Exception as e:
            logger.error(e)
            logger.error(query_select1)
            logger.error('''{0} {1} {2}'''.format((comune, cf_piva[i],codice)))
            logger.error('''{0} {1} {2}'''.format((num_civ[i],let_civ[i],interno)))
        if len(risultato)>=1 : # len(risultato)==1:
            if pandas.isnull(data_cessazione[i]) :
                # presente e ok
                campo_controllo.append('OK')
                w0.write(i0,0,'{}'.format(str(cf_piva[i]).strip()))
                w0.write(i0,1,'{}'.format(cognome_ragsoc[i]))
                w0.write(i0,2,'{}'.format(nome[i]))
                w0.write(i0,3,'{}'.format(codice))
                w0.write(i0,4,'{}'.format(via[i]))
                w0.write(i0,5,'{}'.format(num_civ[i]))
                w0.write(i0,6,'{}'.format(let_civ[i]))
                w0.write(i0,7,'{}'.format(interno[i]))
                i0+=1
            else:
                campo_controllo.append('Da cessare')
                w1.write(i1,0,'{}'.format(str(cf_piva[i]).strip()))
                w1.write(i1,1,'{}'.format(cognome_ragsoc[i]))
                w1.write(i1,2,'{}'.format(nome[i]))
                w1.write(i1,3,'{}'.format(codice))
                w1.write(i1,4,'{}'.format(via[i]))
                w1.write(i1,5,'{}'.format(num_civ[i]))
                w1.write(i1,6,'{}'.format(let_civ[i]))
                w1.write(i1,7,'{}'.format(interno[i]))
                w1.write(i1,8,'{}'.format(data_cessazione[i]))
                i1+=1
        '''if len(risultato) > 1 :
            campo_controllo.append('Da controllare su nostro DB')
            w2.write(i2,0,'{}'.format(str(cf_piva[i]).strip()))
            w2.write(i2,1,'{}'.format(cognome_ragsoc[i]))
            w2.write(i2,2,'{}'.format(nome[i]))
            w2.write(i2,3,'{}'.format(codice))
            w2.write(i2,4,'{}'.format(via[i]))
            w2.write(i2,5,'{}'.format(num_civ[i]))
            w2.write(i2,6,'{}'.format(let_civ[i]))
            w2.write(i2,7,'{}'.format(interno[i]))
            w2.write(i2,8,'{}'.format(data_cessazione[i]))
            i2+=1
        '''
        # non trovato
        if len(risultato) == 0 :
            campo_controllo.append('Non trovato su nostro DB')
            w3.write(i3,0,'{}'.format(str(cf_piva[i]).strip()))
            w3.write(i3,1,'{}'.format(cognome_ragsoc[i]))
            w3.write(i3,2,'{}'.format(nome[i]))
            w3.write(i3,3,'{}'.format(codice))
            w3.write(i3,4,'{}'.format(via[i]))
            w3.write(i3,5,'{}'.format(num_civ[i]))
            w3.write(i3,6,'{}'.format(let_civ[i]))
            w3.write(i3,7,'{}'.format(interno[i]))
            w3.write(i3,8,'{}'.format(data_cessazione[i]))
            nt+=1
            query_select2= '''SELECT CF_PIVA, cognome, nome, ufg.CODICE_VIA, s.NOME2,
                CIVICO,LETTERA_CIVICO, INTERNO 
                FROM STRADE.UTENZE_FUORI_GENOVA ufg 
                JOIN STRADE.COMUNI c ON c.ID_COMUNE = ufg.ID_COMUNE 
                JOIN strade.STRADE s ON s.CODICE_VIA =ufg.CODICE_VIA
                WHERE c.PREFISSO_UTENTI = :c1 
                AND CF_PIVA = :c2'''
            try:
                cur2.execute(query_select2, (comune,str(cf_piva[i])))
                risultato2=cur2.fetchall()
                #macro_tappe.append(tappa[2])
            except Exception as e:
                logger.error('''{}{}{}{}{}{}'''.format((comune,cf_piva[i])))
                logger.error(query_select2)
                logger.error(e)    
            if len(risultato2)>0:
                for rr in risultato2:
                    k=0
                    while k < len(rr):
                        w3.write(i3, 9+k, rr[k]) # scrivo tutte le colonne
                        k+=1
                    i3+=1
            else:
                i3+=1
                 
        i+=1
    
    
    workbook.close()

    #exit() # per ora esco qua e non vado oltre

    
    # Create a secure SSL context
    context = ssl.create_default_context()

    ##sender_email = user_mail
    receiver_email='assterritorio@amiu.genova.it'
    debug_email='roberto.marzocchi@amiu.genova.it'

    subject = "Report utenze {}".format(comune)
       
    body = """
    Come da allegato. <br>
    <b>Statistiche comune {6}</b>:<br>
    Totale utenze presenti su nostro DB (fine 2021):{5}<br>
    Totale record file excel: {4}<br
    <hr> 
    Record attivi trovati: {1}%<br>
    Record da cessare trovati: {2}%<br> 
    Record non trovati:{3}%<br>
    <hr><br><br>
    L'applicativo  è stato realizzato dal gruppo Gestione Applicativi del SIGT.<br> 
    Segnalare tempestivamente eventuali malfunzionamenti inoltrando la presente mail a {0}<br><br>
    AMIU Assistenza Territorio<br>
    <img src="cid:image1" alt="Logo" width=197>
    <br>
    """.format(receiver_email, round(i0/len(cf_piva)*100),round(i1/len(cf_piva)*100), round(nt/len(cf_piva)*100), len(cf_piva), tp, comune)
    

    # Create a multipart message and set headers
    message = MIMEMultipart()
    message["From"] = user_mail
    message["To"] = debug_email
    message["Subject"] = subject
    #message["Bcc"] = debug_email  # Recommended for mass emails
    message.preamble = "File comune {}".format(comune)


        
                        
    # Add body to email
    message.attach(MIMEText(body, "html"))


    #aggiungo logo 
    logoname='{}/img/logo_amiu.jpg'.format(path)
    immagine(message,logoname)
    
    
    # aggiunto allegato (usando la funzione importata)
    allegato(message, file_variazioni, nome_file)
    # Add body to email
    message.attach(MIMEText(body, "plain"))
    
    
    text = message.as_string()

    logger.info("Richiamo la funzione per inviare mail")
    invio=invio_messaggio(message)
    logger.info(invio)
    
        
    
    
    
    
    
    
    
    
    
    # check se c_handller contiene almeno una riga 
    error_log_mail(errorfile, 'roberto.marzocchi@amiu.genova.it', os.path.basename(__file__), logger)
    logger.info("chiudo le connessioni in maniera definitiva")
    cur.close()
    con.close()




if __name__ == "__main__":
    main()      