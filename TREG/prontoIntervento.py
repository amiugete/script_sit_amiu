#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2023
# Matteo Scarfò, Roberta Fagandini, Roberto Marzocchi


import ftplib
import os
import sys
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import csv
import psycopg2
from datetime import date
import shutil
import logging
import requests
from requests.exceptions import HTTPError
import uuid
import time
from datetime import datetime, date, time, timezone

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import credenziali
from invio_messaggio import *


def clean(val):
    if val in ('', ' ', None, 'NULL'):
        return None

    # Se è un numero float tipo 3.0 o 3,0 → torna "3"
    if isinstance(val, float):
        if val.is_integer():
            return str(int(val))
        else:
            return str(val).replace('.', ',')  # se vuoi mantenere virgola

    # Se è un intero puro → stringa
    if isinstance(val, int):
        return str(val)

    # Se è stringa tipo "3,0" → normalizza
    s = str(val).strip()
    if s.replace(',', '').replace('.', '').isdigit():
        # Se ha virgola o punto, puliscilo
        s_norm = s.replace(',', '.')
        try:
            f = float(s_norm)
            if f.is_integer():
                return str(int(f))
            else:
                return str(f).replace('.', ',')
        except ValueError:
            pass

    return s





def to_iso_z(d, t=None):
    """Converte una data locale in formato ISO 8601 UTC (suffisso Z reale)"""
    if not d:
        return None

    if isinstance(d, str):
        return None

    if isinstance(d, datetime):
        dt = d
    elif isinstance(d, date):
        dt = datetime.combine(d, t or datetime.min.time())
    else:
        return None

    # Se il datetime è naïve (senza timezone), assumilo locale e converti in UTC
    if dt.tzinfo is None:
        # .astimezone() prende il fuso locale automaticamente
        dt = dt.astimezone()

    # Converti in UTC
    dt_utc = dt.astimezone(timezone.utc)

    # Ritorna in formato ISO con suffisso Z
    return dt_utc.isoformat().replace("+00:00", "Z")



####### CARICAMENTO SU SIT #######

def GetListaFiles(ftp):
    lista = []
    ftp.retrlines('LIST', lambda r: lista.append(r.split()[-1]))        
    return lista

def DownloadFiles(ftp, lista):
    try:         
        for file in lista:       
            with open(os.path.join(download, file), 'wb') as f:
                    ftp.retrbinary(f'RETR {file}', f.write)
            #print(file + ' Salvato con successo') 
        return 1
    except Exception as ex:
         print("ERRORE NEL DOWNLOAD DEL FILE " + file)
         print("ERORRE: ", ex)
         return 0

def GetElencoPercorsi(lista):
    elenco_percorsi = []

    for nome_file in os.listdir(download):
        for file in lista:
            if nome_file == file:
                percorso_completo = os.path.join(download, nome_file)
                elenco_percorsi.append(percorso_completo)
    return elenco_percorsi

def LeggiCsv(percorso_file):
    righe = []

    with open(percorso_file, newline='', encoding='utf-8') as file:
        reader = csv.DictReader(file, delimiter=';')
        for riga in reader:
            righe.append(riga)          
            
    return righe

def RiparaTesto(s):
    for enc in ['latin1', 'windows-1252', 'iso-8859-1']:
        try:
            return s.encode(enc).decode('utf-8')
        except:
            continue
    return s

def RiparaValori(d):    
    def ripara_ricorsivo(val):
        if isinstance(val, str):
            return RiparaTesto(val)
        elif isinstance(val, dict):
            return {k: ripara_ricorsivo(v) for k, v in val.items()}
        elif isinstance(val, list):
            return [ripara_ricorsivo(v) for v in val]
        else:
            return val

    return {k: ripara_ricorsivo(v) for k, v in d.items()}



def send_email(destinatario, oggetto, testo):

    sender_email = "ScriptGapFTP@amiu.genova.it"  
    receiver_email = destinatario

    msg = MIMEMultipart()
    msg['From'] = sender_email
    msg['To'] = ', '.join(destinatari)    
    msg['Subject'] = oggetto

    body = testo
    msg.attach(MIMEText(body, 'plain'))

    smtp_server = "SMTP4Applications.amiu.genova.it"
    smtp_port = 25
   
    server = smtplib.SMTP(smtp_server, smtp_port)    
    text = msg.as_string()
    server.sendmail(sender_email, receiver_email, text)   

    server.quit()


def SalvaFileDB(file, nomefile):

    cursor, conn = ConnettiDB()

    if cursor is None or conn is None:
        raise Exception("Impossibile connettersi al DB!")              

    #print("CONNESSIONE AL DB AVVENUTA CON SUCCESSO")

    for r in file:      
        InserisciRiga(r, nomefile, cursor)
    
    conn.commit()
    DisconttettiDB(cursor, conn)


def InserisciRiga(riga, nomefile, cursor):
    #ricavo le query dal file .sql   
    query = carica_query_da_file("queryProntoIntervento")
    
    riga = RiparaValori(riga) #ripara il testo dove sono presenti lettere accentate    
    
    #insert nel DB
    cursor.execute(query["inserisci_richiesta"], (
    clean(riga['scopo']),
    clean(riga['sottoscopo']),
    clean(riga['esito']),
    clean(riga['data_telefonata']),
    clean(riga['codice_identificativo_segnalazione']),
    clean(riga['tipologia_apertura_guasto']),
    clean(riga['data_apertura_segnalazione']),
    clean(riga['ora_apertura_segnalazione']),
    clean(riga['localita']),
    clean(riga['nominativo_segnalante']),
    clean(riga['recapito_telefonico']),
    clean(riga['tipologia_richiesta']),
    clean(riga['note_chiusura']),
    clean(riga['data_chiusura_segnalazione']),
    clean(riga['ora_chiusura_segnalazione']),
    clean(riga['nominativo_tecnico_amiu']),
    clean(riga['identificazione_intervento_alla_chiusura']),
    date.today(),
    nomefile,
    clean(riga['indirizzo']),
    clean(riga['Data_di_arrivo_sul_luogo']),
    clean(riga['Data_di_messa_in_sicurezza_del_sito']),
    clean(riga['Data_di_rimozione_dei_rifiuti']),
    clean(riga['Motivazioni_ritardo']),
    clean(riga['Ora_di_arrivo_sul_luogo']),
    clean(riga['Ora_di_messa_in_sicurezza_del_sito']),
    clean(riga['Ora_di_rimozione_dei_rifiuti']),
    ))
        


def ConnettiDB():
    
    conn = psycopg2.connect(
        host="172.24.4.39",
        database="sit",
        user="gisamiu",
        password="gisamiu"           
    )

    cursor = conn.cursor()

    #print("CONNESSO AL DB")

    return cursor, conn

def DisconttettiDB(cursor, conn):
    cursor.close()
    conn.close()

    #print("DISCONNESSO DAL DB")
        

def ScriviSuSit(elenco_percorsi):
    #itero sui file da per leggere il contenuto
    for f in elenco_percorsi:
        file = LeggiCsv(f)
        nomefile = os.path.basename(f)        
        SalvaFileDB(file, nomefile)

def SetInvatoTreg(richieste, errori):
    cursor, conn = ConnettiDB()

    query = carica_query_da_file("queryProntoIntervento")
    
    #scorro le richieste e controllo se hanno gli id di quelle incomplete cosi le rimuovo
    #in modo da non flaggarle sul DB come inviate
    for r in richieste:
        for e in errori:
            if r["id_rich"] == e:
                richieste.remove(r)

    #flaggo le richieste come inviate
    for r in richieste:       
       cursor.execute(query["set_inviato"], (r["id_rich"],))         
    
    conn.commit()

    DisconttettiDB(cursor, conn)



def carica_query_da_file(nome_file):
    percorso_base = os.path.dirname(__file__)  
    percorso_completo = os.path.join(percorso_base, nome_file)
    
    query_dict = {}
    with open(percorso_completo, encoding='utf-8') as f:
        contenuto = f.read()

    blocchi = contenuto.split('-- name:')
    for blocco in blocchi[1:]:
        righe = blocco.strip().splitlines()
        nome = righe[0].strip()
        query = '\n'.join(righe[1:]).strip()
        query_dict[nome] = query

    return query_dict  

def SpostaFileFTP(ftp, file_locale, cartella_dest):
    conn = ftp
    nome = file_locale
    # Costruisci percorso completo destinazione
    dest = f"{cartella_dest.rstrip('/')}/{nome}"
    try:
        conn.rename(nome, dest)        
    except Exception as e:
        print(f"Errore spostamento: {e}")

def SvuotaCartella(percorso):   
    for nome in os.listdir(percorso):
        full_path = os.path.join(percorso, nome)
        if os.path.isfile(full_path):
            try:
                os.remove(full_path)                
            except Exception as e:
                print(f"Errore eliminando {full_path}: {e}")



####### INVIO A TREG #######

def GetRichieste(cursor):
    query = carica_query_da_file("queryProntoIntervento")

    cursor.execute(query["get_richieste"], (date.today().year,))    
    col_names = [desc[0] for desc in cursor.description]  
    result = [dict(zip(col_names, row)) for row in cursor.fetchall()]

    return result


def CreaListaInterventi(lista):
    lista_interventi = []

    nomi_file = [] #file che hanno dei valori obbligatori mancanti
    
    for i in lista:
        
        if i["cod_ident_segn"] is None or i["data_telefonata"] is None or i["emerg_type"] is None or i["istat_code"] is None:
            nomi_file.append(i["id_rich"])
        else:
            receptionDate = to_iso_z(i["data_telefonata"])
            arrivalDateTime = to_iso_z(i["data_arrivo_luogo"], i["ora_arrivo_luogo"]) if i["data_arrivo_luogo"] else None
            securingDateTime = to_iso_z(i["data_messa_sic"], i["ora_messa_sic"]) if i["data_messa_sic"] else None
            cleanUpDateTime = to_iso_z(i["data_rim_rif"], i["ora_rim_rif"]) if i["data_rim_rif"] else None

            intervento = {
                'id': i["cod_ident_segn"],
                'year': i["anno"],
                'receptionDate': receptionDate,  
                'emergencyType': i["emerg_type"],
                'caller': i["nomin_segn"] if i["nomin_segn"] else None,
                'phone': i["rec_tel"] if i["rec_tel"] else None,
                'istatCode': i["istat_code"],
                'address': i["indirizzo"] if i["indirizzo"] else None,
                'nonComplianceCause': i["causale"] if i["causale"] else None,
                'arrivalDateTime': arrivalDateTime,
                'securingDateTime':  securingDateTime if securingDateTime is not None else arrivalDateTime,
                'cleanUpDateTime': cleanUpDateTime,
            }

            lista_interventi.append(intervento)

    return lista_interventi, nomi_file



def token_treg():
    api_url='{}atrif/api/v1/tobin/auth/login'.format(credenziali.url_ws_treg)
    payload_treg = {"username": credenziali.user_ws_treg, "password": credenziali.pwd_ws_treg, }
    logger.debug(payload_treg)
    response = requests.post(api_url, json=payload_treg)
    logger.debug(response)
    #response.json()
    logger.info("Status code: {0}".format(response.status_code))
    try:      
        response.raise_for_status()
        # access JSOn content
        #jsonResponse = response.json()
        #print("Entire JSON response")
        #print(jsonResponse)
    except HTTPError as http_err:
        logger.error(f'HTTP error occurred: {http_err}')
        check=500
    except Exception as err:
        logger.error(f'Other error occurred: {err}')
        logger.error(response.json())
        check=500
    token=response.text
    return token

import inspect 


# Create a custom logger
giorno_file=datetime.today().strftime('%Y%m%d_%H%M%S')
filename = inspect.getframeinfo(inspect.currentframe()).filename
path=os.path.dirname(sys.argv[0]) 
path1 = os.path.dirname(os.path.dirname(os.path.abspath(filename)))
nome=os.path.basename(__file__).replace('.py','')
#tmpfolder=tempfile.gettempdir() # get the current temporary directory
logfile='{0}/log/{2}_{1}.log'.format(path,nome,giorno_file)
errorfile='{0}/log/{2}_error_{1}.log'.format(path,nome,giorno_file)
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
#f_handler = logging.StreamHandler()
f_handler = logging.FileHandler(filename=logfile, encoding='utf-8', mode='w')


c_handler.setLevel(logging.ERROR)
f_handler.setLevel(logging.DEBUG)


# Add handlers to the logger
logger.addHandler(c_handler)
logger.addHandler(f_handler)


cc_format = logging.Formatter('%(asctime)s\t%(levelname)s\t%(message)s')

c_handler.setFormatter(cc_format)
f_handler.setFormatter(cc_format)

download = "TREG/download"

destinatari = ["matteo.scarfo@amiu.genova.it", "AssTerritorio@amiu.genova.it"]

def main():
    try:        
        MAX_RETRIES = 5
        DELAY_SECONDS = 10
        
        #connessione all'ftp 
        
        logger.info("Connessione all'FTP...")

        ftp = ftplib.FTP_TLS()
        ftp.connect(credenziali.servergGi, credenziali.portaGi, timeout=10)
        ftp.auth()  
        ftp.login(credenziali.userGi, credenziali.pwdGi)
        ftp.prot_p()     

        #download dei file       

        lista = GetListaFiles(ftp) #mi ricavo i nomi dei file da scaricare 
        lista = [f for f in lista if f.endswith(".csv")]   
        
        if lista:     
            logger.info("Download dei file") 
            val = DownloadFiles(ftp, lista) #scarico i file
            
            if val == 0:
                send_email(destinatari, "Script Pronto intervento", "Pronto intervento - Fallito il download dei file" )   
                logger.error("Fallito il download dei file")
                sys.exit()
        
            #ricavo i nomi dei pecorsi dei file scaricati
            elenco_percorsi = GetElencoPercorsi(lista)      

            logger.info("Scrittura sul DB dei dati...")
            ScriviSuSit(elenco_percorsi)          

            lista = GetListaFiles(ftp)
            lista = [f for f in lista if f.endswith(".csv") or f.endswith(".sig")]

            logger.info("Spostamento dei file nella cartella archivio dell'FTP...")
            for f in lista:
               SpostaFileFTP(ftp, f, '/archivio')        

            SvuotaCartella(download) #svuoto la cartella di download in locale

            ftp.quit()  
        else:
            #send_email(destinatari, "Script Pronto intervento", "Pronto intervento - Non sono stati trovati file da caricare" )   
            logger.info("Non ci sono file da caricare")
            ftp.quit()
        
        
        #Prendo solo le chiamate con scopo Pronto intervento con stato 0 ovvero non inviate 
        
        logger.info("Recupero richieste pronto intervento...")
        cursor, conn = ConnettiDB()

        richieste = GetRichieste(cursor)  

        DisconttettiDB(cursor, conn)

        interventi, errori = CreaListaInterventi(richieste)              

        if interventi:
            #Connessione con Token a TREG

            logger.info("Connessione a TREG...")

            token = token_treg()
            #logger.debug(token)

            guid = uuid.uuid4()
            #logger.debug(str(guid))

            json_id = { 'id': str(guid) }
            api_url_begin_upload = '{}atrif/api/v1/tobin/b2b/process/rifqt-emergencies/begin-upload/av1'.format(credenziali.url_ws_treg)
            response = requests.post(api_url_begin_upload, json=json_id, headers={'accept': '*/*',
                                                                                'mde': 'PROD',
                                                                                'Authorization': 'EIP {}'.format(token),
                                                                                'Content-Type': 'application/json'})         

            importId = response.json()['importId']

            logger.info('ImportId = {}'.format(importId))
            
            logger.info("Connessione avvenuta")     

            
            #Upload delle richieste

            logger.info("Invio a TREG in corso...")
            
            api_url_upload='{}atrif/api/v1/tobin/b2b/process/rifqt-emergencies/upload/av1'.format(credenziali.url_ws_treg)

            body_upload = {
                'id': str(guid),
                'importId': str(importId),
                'entities': interventi
            }

            try: 
                for attempt in range(1, MAX_RETRIES + 1):
                    if attempt> 1:
                        logger.warning(f"Tentativo {attempt}")
                    
                    response_upload =  requests.post(api_url_upload, json=body_upload, headers={'accept': '*/*',
                                                                                                'mde': 'PROD',
                                                                                                'Authorization': 'EIP {}'.format(token),
                                                                                                'Content-Type': 'application/json'})
                    logger.debug(response_upload.text)
                    
                    if response_upload.json()['errorCount'] != 0:
                        logger.error(response_upload.text)
                        check_error_upload += response_upload.json()['errorCount']
                    break
            except Exception as ex:
                logger.warning(ex)
                if attempt == MAX_RETRIES:
                            logger.error("Tutti i tentativi sono falliti. Operazione interrotta.")
                            raise ValueError(ex)  # fermo l'esecuzione
                else:
                    time.sleep(DELAY_SECONDS)

            if response_upload.json()['errorCount'] == 0:
                #commit delle richieste
                try:
                    logger.info('Inizio il commit degli upload su TREG')
                    api_url_commit_upload='{}atrif/api/v1/tobin/b2b/process/rifqt-emergencies/commit-upload/av1'.format(credenziali.url_ws_treg)
                    body_commit_upload={
                            'id': str(guid),
                            'importId': str(importId)
                        }                
                    response_commit_upload = requests.post(api_url_commit_upload, json=body_commit_upload, headers={'accept':'*/*', 
                                                                                        'mde': 'PROD',
                                                                                        'Authorization': 'EIP {}'.format(token),
                                                                                        'Content-Type': 'application/json'})                
                    logger.info('Fine commit - Risposta TREG: {}'.format(response_commit_upload.text))

                    SetInvatoTreg(richieste, errori)  #segna le richieste come inviate a Treg

                    if not errori: # "errori" contiene gli id delle richieste incomplete
                        send_email(destinatari, "Script Pronto intervento", "Pronto intervento - Script eseguito con sucesso" )
                    else:
                        send_email(destinatari, "Script Pronto intervento", "Pronto intervento - Script eseguito con successo ma alcune richieste erano incomplete, ecco gli id: " + str(errori))
                        logger.info("Completato invio a TREG")
                except Exception as ex:
                    logger.error(ex)                                
            else:
                send_email(destinatari, "Script Pronto intervento", "Pronto intervento - Lo script è terminato con errori: " +  response_upload.text)
        else:
            logger.info("Non sono state trovare richieste da inviare nel DB")
    except Exception as ex:
        logger.error(ex)
    finally:
        if cursor or conn:
            DisconttettiDB(cursor, conn)
        if ftp.sock:
            ftp.quit()

    
    # check se c_handller contiene almeno una riga 
    error_log_mail(errorfile, 'assterritorio@amiu.genova.it', os.path.basename(__file__), logger)
        

if __name__ == "__main__":
    main()
