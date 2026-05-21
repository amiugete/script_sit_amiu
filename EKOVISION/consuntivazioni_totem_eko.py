#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2023
# Roberto Marzocchi

'''
Script 

'''

#from msilib import type_short
import os, sys, re  # ,shutil,glob


import requests
from requests.exceptions import HTTPError

import json

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



# per mandare file a EKOVISION
import pysftp


#import requests

import logging




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



import uuid





def main():
    
    
    try:
        if sys.argv[1]== 'prod':
            test=0
        elif sys.argv[1]== 'test':
            test=1
        else: 
            print('Il parametro {} passato non è riconosciuto'.format(sys.argv[1]))
            exit()
    except Exception as e:
        test=1
    
    path=os.path.dirname(sys.argv[0]) 
    nome=os.path.basename(__file__).replace('.py','')
    #tmpfolder=tempfile.gettempdir() # get the current temporary directory
    if test==0:
        logfile='{0}/log/{1}.log'.format(path,nome)
        errorfile='{0}/log/error_{1}.log'.format(path,nome)
    else: 
        logfile='{0}/log/{1}_test.log'.format(path,nome)
        errorfile='{0}/log/error_{1}_test.log'.format(path,nome)
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
    f_handler.setLevel(logging.INFO)


    # Add handlers to the logger
    logger.addHandler(c_handler)
    logger.addHandler(f_handler)


    cc_format = logging.Formatter('%(asctime)s\t%(levelname)s\t%(message)s')

    c_handler.setFormatter(cc_format)
    f_handler.setFormatter(cc_format)
    
    if test==1:
        logger.info('Ambiente di TEST')
      
    logger.info('Il PID corrente è {0}'.format(os.getpid()))
    
    
    
    
    
    
    
        
    # Mi connetto al nuovo DB consuntivazione  
    if test ==1:
        nome_db= db_totem_test
        URL_WS= eko_url_test
    elif test==0:
        nome_db=db_totem
        URL_WS= eko_url
    else:
        logger.error(f'La variabilie test vale {test}. Si tratta di un valore anomalo. Mi fermo qua')
        exit()
        
    logger.info('Connessione al db {} su {}'.format(nome_db, host_totem))
    conn_c = psycopg2.connect(dbname=nome_db,
                        port=port,
                        user=user_totem,
                        password=pwd_totem,
                        host=host_totem)

    
    curr_c = conn_c.cursor()
    
    nome_db=db
    logger.info('Connessione al db {}'.format(nome_db))
    conn = psycopg2.connect(dbname=nome_db,
                        port=port,
                        user=user,
                        password=pwd,
                        host=host)


    curr = conn.cursor()
    
    
    # WS EKO
    headers = {'Content-Type': 'application/x-www-form-urlencoded'}
    auth_data_eko={'user': eko_user, 'password': eko_pass, 'o2asp' :  eko_o2asp}
    
    
    
    schemi={'raccolta', 'spazzamento'}
    
    

    for schema in schemi: 
        ####################################################################################################
        logger.info(f"Update {schema}")

        
        logger.info('Definisco le query')
        sql_select='''select distinct id_percorso as cod_percorso, 
        to_char(datalav, 'YYYYMMDD'), 
        e.causale_sit, 
        vc.id_ekovision, 
        e.username
        from {}.percorsi_non_effettuati_x_ekovision e 
        join totem.v_causali vc on vc.id = e.causale_sit 
        where ws_ok is not true and ko_from_eko is not true
        '''.format(schema)    
        
        
        mail_sit='''select email from util.sys_users where "name" like %s '''

        update= '''
            update {}.percorsi_non_effettuati_x_ekovision
            set ws_ok = true
            where id_percorso = %s and 
            datalav = to_date(%s, 'YYYYMMDD')
            '''.format(schema)
        
        
        update2= '''
            update {}.percorsi_non_effettuati_x_ekovision
            set ko_from_eko = true
            where id_percorso = %s and 
            datalav = to_date(%s, 'YYYYMMDD')
            '''.format(schema)    
        
        
        
            
        logger.info('Ok Procedo')
        
        try:
            curr_c.execute(sql_select)
            schede_eseguite=curr_c.fetchall()
        except Exception as e:
            logger.error(sql_select)
            logger.error(e)
        
        

        
        for se in schede_eseguite:
            
            logger.info(f'Recupero id scheda da cod_percorso {se[0]} e data {se[1]}')
            #exit()
            # provo il WS solo con la data 
            params={'obj':'schede_lavoro',
                'act' : 'r',
                'sch_lav_data': se[1],
                'cod_modello_srv': se[0],
                'flg_includi_eseguite': 1,
                'flg_includi_chiuse': 1
                }
            response = requests.post(URL_WS, params=params, data=auth_data_eko, headers=headers)
            #response.json()
            logger.debug(response.status_code)
            try:      
                response.raise_for_status()
                check=0
                # access JSOn content
                #jsonResponse = response.json()
                #print("Entire JSON response")
                #print(jsonResponse)
            except HTTPError as http_err:
                logger.error(f'HTTP error occurred: {http_err}')
                check=1
            except Exception as err:
                logger.error(f'Other error occurred: {err}')
                logger.error(response.json())
                check=1
            if check<1:
                letture = response.json()
                #logger.info(letture)
                logger.info(' Trovate {} schede di lavoro su Ekovision'.format(len(letture['schede_lavoro'])))
                #logger.debug(letture['schede_lavoro'])
                #exit()
                #logger.debug(len(letture['schede_lavoro']))
                
                if len(letture['schede_lavoro']) == 0:
                    #va creata la scheda di lavoro
                    logger.error(f'Per il percorso {se[0]} del {se[1]} andrebbe creata la scheda di lavoro')
                    error_log_mail(errorfile, 'assterritorio@amiu.genova.it', os.path.basename(__file__), logger)
                    exit()
                elif len(letture['schede_lavoro']) ==1 : 
                    logger.debug('La schede di lavoro esiste' )
                    id_scheda=letture['schede_lavoro'][0]['id_scheda_lav']
                    logger.info(id_scheda)
        
                elif len(letture['schede_lavoro']) > 1 : 
                    logger.error(f'''Per la il percorso {se[0]} del {se[1]} esiste più di una scheda su eko, 
                                non riesco a passare consuntivazione del totem. Da gestire meglio errore''')
                    error_log_mail(errorfile, 'assterritorio@amiu.genova.it', os.path.basename(__file__), logger)
                    exit()
                
        
                
                
                logger.info('Provo a leggere i dettagli della scheda {}'.format(id_scheda))
            
                
                params2={'obj':'schede_lavoro',
                        'act' : 'r',
                        'id': '{}'.format(id_scheda),
                        }
                
                response2 = requests.post(URL_WS, params=params2, data=auth_data_eko, headers=headers)
                #letture2 = response2.json()
                letture2 = response2.json()
                #logger.info(letture2)
                #exit()
                # key to remove
                #key_to_remove = "status"
                del letture2["status"]  
                del letture2['schede_lavoro'][0]['trips']  
                del letture2['schede_lavoro'][0]['risorse_tecniche']
                del letture2['schede_lavoro'][0]['risorse_umane']
                del letture2['schede_lavoro'][0]['filtri_rfid']        
                #logger.info(letture2)
                
                #logger.info(json.dumps(letture2).encode("utf-8"))
                
                
                if letture2['schede_lavoro'][0]['flg_eseguito'] == "0":
                
                    letture2['schede_lavoro'][0]['servizi'][0]['flg_segn_srv_non_effett']="1"
                    letture2['schede_lavoro'][0]['servizi'][0]['txt_segn_srv_non_effett']="Non effettuata da totem"
                    letture2['schede_lavoro'][0]['servizi'][0]['id_caus_srv_non_eseg']=se[3]
                    letture2['schede_lavoro'][0]['flg_eseguito']='1'
                    letture2['schede_lavoro'][0]['flg_imposta_eseguito']='1'
                
                
                    
                    
                    
                    #exit()
                    
                    logger.info('Provo a salvare nuovamente la scheda {} con causale {}'.format(id_scheda, se[3]))
                    
                    
                    guid = uuid.uuid4()

                    params2={'obj':'schede_lavoro',
                            'act' : 'w',
                            'ruid': '{}'.format(str(guid)),
                            'json': json.dumps(letture2, ensure_ascii=False).encode('utf-8')
                            }
                    #exit()
                    response2 = requests.post(URL_WS, params=params2, data=auth_data_eko, headers=headers)
                    result2 = response2.json()
                    if result2['status']=='error':
                        logger.error('Id_scheda = {}'.format(id_scheda))
                        logger.error(result2)
                    else :
                        logger.info('Aggiorno il db')
                        
                        try:
                            curr_c.execute(update, (se[0], se[1]))
                        except Exception as e:
                            logger.error(update)
                            logger.error(e)
                
                else: 
                    messaggio_warning= f'''Gent. {se[4]}<br> 
                                   <br>Consuntivazione: <b>{schema}</b>
                                   <br>Hai consuntivato la seguente scheda come non effettuata su totem. 
                                   <br>Scheda {id_scheda} (cod_percorso = {se[0]} data {datetime.strptime(se[1],'%Y%m%d' ).strftime('%d/%m/%Y')})
                                   <br>Tale scheda risulta già eseguita su Ekovision, quanto indicato su totem non può essere inviato a Ekovision.
                                   Verificare manualmente la consuntivazione della scheda di lavoro su Ekovision e se necessario correggerla'''
                    logger.warning(messaggio_warning)
                    
                    # recupero la mail da SIT
                    try:
                        curr.execute(mail_sit, (se[4],))
                        mail_sit_result=curr.fetchone()[0]
                        logger.info(f'Mail recuperata da SIT per l\'utente {se[4]}: {mail_sit_result}')
                        warning_message_mail(messaggio_warning, mail_sit_result, os.path.basename(__file__), logger, 'ATTENZIONE: scheda già eseguita su Ekovision')
                        #exit()
                    except Exception as e:
                        logger.error(mail_sit)
                        logger.error(e)
                        mail_sit_result=None
                
                
                
                    try:
                        curr_c.execute(update2, (se[0], se[1]))
                    except Exception as e:
                        logger.error(update2)
                        logger.error(e)
                    
        
    
    
    
    
    # ora controllo anche eventuali autisti da rimuovere da Ekovision 
    # funzionalità pensata per le rimesse che non devono poter chiudere direttamente le schede
    # ma solo impostare una causale sull'autista che non ha effettuato il servizio
    # sarà poi l'UT che eventualmente darà la scheda come non effettuata su totem e quindi su Eko 
    
    sql_select_auti='''select distinct id_percorso as cod_percorso, 
        to_char(datalav, 'YYYYMMDD'), 
        username 
        from raccolta.percorsi_no_autista_x_ekovision e 
        where ws_ok is not true and ko_from_eko is not true
        '''  
            

    update_auti= '''
        update raccolta.percorsi_no_autista_x_ekovision
        set ws_ok = true
        where id_percorso = %s and 
        datalav = to_date(%s, 'YYYYMMDD')
        '''
        
    
    
    update2_auti= '''
        update raccolta.percorsi_no_autista_x_ekovision
        set ko_from_eko = true
        where id_percorso = %s and 
        datalav = to_date(%s, 'YYYYMMDD')
        '''

    
    
    try:
        curr_c.execute(sql_select_auti)
        schede_no_auti=curr_c.fetchall()
    except Exception as e:
        logger.error(sql_select)
        logger.error(e)
    
    
    logger.info('Procedo con eventuali autisti da rimuovere')
    
    for se_na in schede_no_auti:
        
        logger.info(f'Recupero id scheda da cod_percorso {se_na[0]} e data {se_na[1]}')
        #exit()
        # provo il WS solo con la data 
        params={'obj':'schede_lavoro',
            'act' : 'r',
            'sch_lav_data': se_na[1],
            'cod_modello_srv': se_na[0],
            'flg_includi_eseguite': 1,
            'flg_includi_chiuse': 1
            }
        response = requests.post(URL_WS, params=params, data=auth_data_eko, headers=headers)
        #response.json()
        logger.debug(response.status_code)
        try:      
            response.raise_for_status()
            check=0
            # access JSOn content
            #jsonResponse = response.json()
            #print("Entire JSON response")
            #print(jsonResponse)
        except HTTPError as http_err:
            logger.error(f'HTTP error occurred: {http_err}')
            check=1
        except Exception as err:
            logger.error(f'Other error occurred: {err}')
            logger.error(response.json())
            check=1
        if check<1:
            letture = response.json()
            #logger.info(letture)
            logger.info(' Trovate {} schede di lavoro su Ekovision'.format(len(letture['schede_lavoro'])))
            #logger.debug(letture['schede_lavoro'])
            #exit()
            #logger.debug(len(letture['schede_lavoro']))
            
            if len(letture['schede_lavoro']) == 0:
                #va creata la scheda di lavoro
                logger.error('Andrebbe creata la scheda di lavoro')
                error_log_mail(errorfile, 'assterritorio@amiu.genova.it', os.path.basename(__file__), logger)
                exit()
            elif len(letture['schede_lavoro']) ==1 : 
                logger.debug('La schede di lavoro esiste' )
                id_scheda=letture['schede_lavoro'][0]['id_scheda_lav']
                logger.info(id_scheda)
    
            elif len(letture['schede_lavoro']) > 1 : 
                logger.error(f'''Per la il percorso {se[0]} del {se[1]} esiste più di una scheda su eko, 
                            non riesco a passare consuntivazione del totem. Da gestire meglio errore''')
                error_log_mail(errorfile, 'assterritorio@amiu.genova.it', os.path.basename(__file__), logger)
                exit()
            
    
            
            
            logger.info('Provo a leggere i dettagli della scheda {}'.format(id_scheda))
        
            
            params2={'obj':'schede_lavoro',
                    'act' : 'r',
                    'id': '{}'.format(id_scheda),
                    }
            
            response2 = requests.post(URL_WS, params=params2, data=auth_data_eko, headers=headers)
            #letture2 = response2.json()
            letture2 = response2.json()
            #logger.info(letture2)
            #exit()
            # key to remove
            #key_to_remove = "status"
            del letture2["status"]  
            del letture2['schede_lavoro'][0]['trips']  
            del letture2['schede_lavoro'][0]['risorse_tecniche']
            del letture2['schede_lavoro'][0]['filtri_rfid']        
            #logger.info(letture2)
            
            #logger.info(json.dumps(letture2).encode("utf-8"))
            
            i_ru=0
            while i_ru < len(letture2['schede_lavoro'][0]['risorse_umane']):
                # verifico se la mansione è autista 33
                if letture2['schede_lavoro'][0]['risorse_umane'][i_ru]['id_mansione'] == 33 and letture2['schede_lavoro'][0]['risorse_umane'][i_ru]['flg_autista']=="1":
                    # imposto il giustificativo 5 Operatore non disponibile (percorso fatto ugualmente)
                    letture2['schede_lavoro'][0]['risorse_umane'][i_ru]['id_giustificativo']=5
                    letture2['schede_lavoro'][0]['risorse_umane'][i_ru]['txt_giustificativo']="Autista reso non disponibile da totem"
                i_ru+=1
            
                
                
                
            #exit()
            
            logger.info('''Provo a salvare nuovamente la scheda {} con il giustificativo sull'autista '''.format(id_scheda))
            
            
            guid = uuid.uuid4()

            params2={'obj':'schede_lavoro',
                    'act' : 'w',
                    'ruid': '{}'.format(str(guid)),
                    'json': json.dumps(letture2, ensure_ascii=False).encode('utf-8')
                    }
            #exit()
            response2 = requests.post(URL_WS, params=params2, data=auth_data_eko, headers=headers)
            result2 = response2.json()
            if result2['status']=='error':
                logger.error('Id_scheda = {}'.format(id_scheda))
                logger.error(result2)
            else :
                logger.info('Aggiorno il db')
                
                try:
                    curr_c.execute(update, (se_na[0], se_na[1]))
                except Exception as e:
                    logger.error(update)
                    logger.error(e)
    
    
    
    
    # faccio commit
    conn_c.commit()
    
    
    # check se c_handller contiene almeno una riga 
    error_log_mail(errorfile, 'assterritorio@amiu.genova.it', os.path.basename(__file__), logger)
    logger.info("chiudo le connessioni in maniera definitiva")
    
    curr_c.close()
    #currc1.close()
    conn_c.close()
    





if __name__ == "__main__":
    main()