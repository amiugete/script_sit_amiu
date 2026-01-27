
import requests
from requests.exceptions import HTTPError

import time 
import os

from invio_messaggio import *
from credenziali import *
import uuid

from datetime import date, datetime, timedelta, timezone, time


def token_treg(logger):
    api_url='{}atrif/api/v1/tobin/auth/login'.format(url_ws_treg)
    payload_treg = {"username": user_ws_treg, "password": pwd_ws_treg, }
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


def call_treg_api(tk, url, body, content_debug, logger, errorfile, error_param, importid):
    
    '''
    Docstring for call_treg_api
    
    :param tk: token treg
    :param url: url metodo treg 
    :param body: body payload 
    :param content_debug: lista di valori in errore
    :param logger: logger
    :param errorfile: nome file errore definito nel main
    :param error_param: parametro da controllare per vedere se la chiamata va in errore, 
        purtroppo ogni metodo restituisce roba diversa ed è difficile da generalizzare uso un if
    '''
    check_error_upload=0
    
    MAX_RETRIES = 5  # Numero massimo di tentativi
    DELAY_SECONDS = 10  # Tempo di attesa tra i tentativi
    
    
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            
            if attempt> 1:
                logger.warning(f"Tentativo {attempt}")
            
            # 🔁 CODICE CHE PUÒ FALLIRE
            response_m = requests.post(url, json=body, headers={'accept':'*/*', 
                                                                    'mde': 'PROD',
                                                                    'Authorization': 'EIP {}'.format(tk),
                                                                    'Content-Type': 'application/json'})
            
            logger.debug(response_m.text)
            #logger.debug(response_m.json()['errorCount'])
            #exit()
            
            # controllo che non ci siano errori (nel caso mi stoppo)
            #logger.debug('Errore : {}'.format(response_m.json()[error_param]))
            if error_param == 'errorCount':
                logger.debug("Sto facendo l'upload")
                if response_m.json()[error_param]:
                    logger.debug('Errore nella chiamata a TREG') 
                    logger.error(response_m.text)
                    logger.error(content_debug)  
                    
                    # butto il dato su check_error_upload
                    check_error_upload+=1
                # ✅ Se funziona, esci dal ciclo
                break
            # nel caso di delete l'error mi viene restituito in maniera diversa non con errorCount, 
            # ma in caso di errore non ho il deleteCount
            elif error_param == 'deletedCount':
                logger.debug("Sto facendo il delete")
                if response_m.json()[error_param] >=0 : # controllo se mi restituisce un numero. perchè comunque se non ho il trac_code da eliminare avrei 0
                    # ✅ Se funziona, esci dal ciclo
                    #logger.debug('Entro anche qua')
                    break
                else:
                    logger.debug('Errore nella chiamata a TREG') 
                    logger.error(response_m.text)
                    logger.error(content_debug)  
                    
                    # butto il dato su check_error_upload
                    check_error_upload+=1
                

        except Exception as e:
            logger.warning(e)

            if attempt == MAX_RETRIES:
                logger.error("Tutti i tentativi sono falliti, operazione interrotta. Errore: {}".format(e))
                logger.error("dati passati a TREG: {}".format(body))
                logger.error("i seguenti valori generano l'errore: {}".format(content_debug))
                error_log_mail(errorfile, 'assterritorio@amiu.genova.it', os.path.basename(__file__), logger)
                split_url = url.split("/")
                split_url[-2] = "rollback-upload"
                api_url_rollback = "/".join(split_url)
                #api_url_rollback='{}atrif/api/v1/tobin/b2b/process/rifqt-wastecollections/rollback-upload/av1'.format(url_ws_treg)
                # questa sarà da passare a TREG, le altre no
                guid = uuid.uuid4()
                body_rollback={
                    'id': str(guid),
                    'importId': str(importid),
                }
                response_roll = requests.post(api_url_rollback, json=body_rollback, headers={'accept':'*/*', 
                                                                    'mde': 'PROD',
                                                                    'Authorization': 'EIP {}'.format(tk),
                                                                    'Content-Type': 'application/json'})
                logger.error('la chiamata di rollback ha dato questo esito: {}'.format(response_roll.text))
                exit()  # fermo l'esecuzione
            else:
                time.sleep(DELAY_SECONDS)  # Aspetta prima del prossimo tentativo
        
    return check_error_upload


def causale_arera(cursor, causale, logger, errorfile):
    query=''' select ca.id_treg, cd.descrizione 
from etl.cause_disserv cd 
left join etl.causali_arera ca on ca.id = cd.id_causale_arera  
where cd.codice = %s'''

    try:
        cursor.execute(query, (causale,))
        riga=cursor.fetchone()
        #h_inizio=cursor.fetchone()[1]
        #h_fine=cursor.fetchone()[2]
    except Exception as e:
        logger.error(query)
        logger.error(e)
    
    if riga:
        return riga[0]
    else: 
        #logger.error(f'Causale: {causale}')
        # check se c_handller contiene almeno una riga 
        #error_log_mail(errorfile, 'assterritorio@amiu.genova.it', os.path.basename(__file__), logger)
        #exit()
        return None
    




def programming_start_ending_date(cursor, data, id_turno, gc, logger):
    
    ''' 
    La funzione in base a giorno, id_turno e giorno_competenza restituisce un array con la programmingStartDate e la programmingEndingDate 
    nel formato voluto da TREG
    '''

    # inizializzo l'array di output
    dates=[]
    
    #logger.debug(id_turno)
    # query per tirare fuori l'intervallo con cui calcolare il giorno di fine
    query='''select 
            case
                when fine_ora < inizio_ora then 
                1
                else 
                0
            end,
            lpad(inizio_ora::text,2,'0')||':'||lpad(inizio_minuti::text,2,'0') as h_inizio, 
            lpad(fine_ora::text,2,'0')||':'||lpad(fine_minuti::text,2,'0') as h_fine 
            from elem.turni t 
            where id_turno = %s'''

    try:
        cursor.execute(query, (id_turno,))
        riga=cursor.fetchone()
        #h_inizio=cursor.fetchone()[1]
        #h_fine=cursor.fetchone()[2]
    except Exception as e:
        logger.error(query)
        logger.error(e)
    
    interval = riga[0]
    h_inizio = riga[1]
    h_fine= riga[2]

    hhi, mmi = map(int, h_inizio.split(':'))
    hhf, mmf = map(int, h_fine.split(':'))
    
    #logger.debug(interval)
    #exit()
    #data = data.astimezone(timezone.utc)
    data = datetime.combine(data, datetime.min.time())
    if gc == 0:
        data_inizio= data
    elif gc == -1: 
        data_inizio= data-timedelta(days=1)
    else: 
        logger.error('Come mai gc vale {}'.format(gc))

    data_fine = data_inizio+timedelta(days=interval)
    dt_inizio = data_inizio.replace(hour=hhi, minute=mmi, second=0, microsecond=0).astimezone(timezone.utc)
    dt_fine = data_fine.replace(hour=hhf, minute=mmf, second=0, microsecond=0).astimezone(timezone.utc)    
    
    dates.append(dt_inizio.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z')
    dates.append(dt_fine.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z')
    dates.append(dt_inizio.strftime('%Y'))

    return dates
