#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2023
# Roberto Marzocchi

'''



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

# libreria per scrivere file csv
import csv


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
    
    
    
    if test==1:
        logger.info('Ambiente di TEST')
      
    logger.info('Il PID corrente è {0}'.format(os.getpid()))
    
    
    # Get today's date
    #presentday = datetime.now() # or presentday = datetime.today()
    oggi=datetime.today()
    oggi=oggi.replace(hour=0, minute=0, second=0, microsecond=0)
    oggi=date(oggi.year, oggi.month, oggi.day)
    logger.debug('Oggi {}'.format(oggi))
    
    
    #num_giorno=datetime.today().weekday()
    #giorno=datetime.today().strftime('%A')
    giorno_file=datetime.today().strftime('%Y%m%d%H%M')
    #oggi1=datetime.today().strftime('%d/%m/%Y')
    logger.debug(giorno_file)
    
    
        
    # Mi connetto al nuovo DB consuntivazione  
    if test == 1:
        nome_db= db_totem_test
    elif test== 0:
        nome_db=db_totem
    else:
        logger.error(f'La variabilie test vale {test}. Si tratta di un valore anomalo. Mi fermo qua')
        exit()
        
    logger.info('Connessione al db {} su {}'.format(nome_db, host_totem))
    conn_c = psycopg2.connect(dbname=nome_db,
                        port=port,
                        user=user_totem,
                        password=pwd_totem,
                        host=host_totem)


    
    query_registrazioni= '''select r.codice,
pe.id_ekovision,
pe.cognome, pe.nome,
r.id_percorso, 
to_char(r.datalav, 'YYYYMMDD') as data_percorso, 
r.id_qualifica, 
mmq.id_mansione, 
r.sportello, 
r.datainsert
from servizi.registrazioni r 
left join totem.mapping_mansioni_qualifiche mmq on mmq.id_qualifica=r.id_qualifica 
left join v_personale_ekovision_step1 vpes on r.codice::numeric = vpes.codice_badge 
left join personale_ekovision pe 
	on trim(pe.cognome)  = trim(vpes.cognome)
	and trim(pe.nome) = trim(vpes.nome)
	and pe.dt_nascita = vpes.data_nascita 
where r.send_ekovision is not true'''


    query_percorsi = f''' with registrazioni_agg as ({query_registrazioni})
    select id_percorso, data_percorso, min(datainsert) as data_insert
from registrazioni_agg 
group by id_percorso, data_percorso
order by 3
'''
     
    query_registrazioni_percorso = f'''{query_registrazioni} and id_percorso = %s and datalav = %s order by datainsert'''
     
    curr_c = conn_c.cursor()
  
    check=0   
    headers = {'Content-Type': 'application/x-www-form-urlencoded'}
    
    #headers = {'Content-type': 'application/json;'}

    auth_data_eko={'user': eko_user, 'password': eko_pass, 'o2asp' :  eko_o2asp}
    
    
    

    
    if test == 1 :
        eko_url=eko_url_test
        logger.debug('Uso ambiente di TEST')
    else:
        eko_url=eko_url
    
    #exit()

    try:
        curr_c.execute(query_percorsi)
        percorsi=curr_c.fetchall()
    except Exception as e:
        logger.error(query_percorsi)
        logger.error(e)
    
    
    '''datalav= '20260525'  #oggi.strftime('%Y%m%d')
    cod_percorso= '0101362801'
    
    rt_write = None
    ru_write =  2425
    
    ru_mans = 165
    '''
    
    if len(percorsi) == 0:
        logger.info('Non ci sono percorsi da inviare ad Ekovision')
    else:
        logger.info('Ci sono {0} percorsi da inviare ad Ekovision'.format(len(percorsi)))
        
        #exit()
        for percorso in percorsi:
            cod_percorso=percorso[0]
            datalav=percorso[1]
            logger.info('Elaboro percorso {0} del {1}'.format(cod_percorso, datalav))
            # cerco id della scheda da modificare
    
            params={'obj':'schede_lavoro',
                'act' : 'r',
                'sch_lav_data': datalav,
                'cod_modello_srv': cod_percorso,
                'flg_includi_eseguite': 1,
                'flg_includi_chiuse': 1
                }
            
            
            response = requests.post(eko_url, params=params, data=auth_data_eko, headers=headers)
            #response.json()
            logger.debug(response.status_code)
            try:      
                response.raise_for_status()
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
                logger.info(len(letture['schede_lavoro']))
                logger.debug(letture['schede_lavoro'])
                #exit()
                
                if len(letture['schede_lavoro']) == 0:
                    ##########################################
                    #  TODO
                    ##########################################
                    #va creata la scheda di lavoro
                    logger.info('Andrebbe creata la scheda di lavoro')
                    exit()
                    response2 = requests.post(eko_url, params=params2, data=auth_data_eko, headers=headers)
                    letture2 = response2.json()
                    logger.info(letture2)
                    try: 
                        id_scheda=letture['crea_schede_lavoro'][0]['id']
                    except Exception as e:
                        logger.error(e)
                elif len(letture['schede_lavoro']) > 0 : 
                    id_scheda=letture['schede_lavoro'][0]['id_scheda_lav']
                    turno=letture['schede_lavoro'][0]['cod_turno_ext']
                    in_lavorazione= letture['schede_lavoro'][0]['flg_in_lavorazione']
                    eseguita=int(letture['schede_lavoro'][0]['flg_eseguito'])
                    chiusa= letture['schede_lavoro'][0]['flg_chiuso'] 
                    logger.info(id_scheda)
                    logger.info(turno)
            

            
            if eseguita == 1:
                logger.info('La scheda è già eseguita')
                logger.info ('La scheda è già eseguita, invio mail e non faccio nulla')
                
            
                
            else:     
                
                logger.info('Provo a leggere i dettagli della scheda')
                
                
                params2={'obj':'schede_lavoro',
                        'act' : 'r',
                        'id': '{}'.format(id_scheda),
                        'flg_esponi_consunt': 1
                        }
                
                response2 = requests.post(eko_url_test, params=params2, data=auth_data_eko, headers=headers)
                #letture2 = response2.json()
                letture2 = response2.json()
                #logger.info(letture2)
                #exit()
                # key to remove
                #key_to_remove = "status"
                del letture2["status"]  
                del letture2['schede_lavoro'][0]['trips']  
                # del letture2['schede_lavoro'][0]['risorse_tecniche']
                # del letture2['schede_lavoro'][0]['risorse_umane']   
                del letture2['schede_lavoro'][0]['filtri_rfid']        
                #logger.info(letture2)
                
                
                
                id_rt=[]
                progr_rt=[]
                
                
                
                id_ru=[]
                progr_ru=[]
                flg_auti=[]
                ru_progr_rt=[]
                mansioni= []
                # 0 se non cìè niente
                # 1 se c'è risorsa predefinita Ekovision
                # se già inserito il dato
                tipo_inserimento_ru=[]
                tipo_inserimento_rt=[]
                
                rt=0
                while rt < len(letture2['schede_lavoro'][0]['risorse_tecniche']):
                    id_rt.append(letture2['schede_lavoro'][0]['risorse_tecniche'][rt]['id'])
                    progr_rt.append(letture2['schede_lavoro'][0]['risorse_tecniche'][rt]['id_progressivo'])
                    if int(letture2['schede_lavoro'][0]['risorse_tecniche'][rt]['id']) == 0: 
                        tipo_inserimento_rt.append(0)
                    elif int(letture2['schede_lavoro'][0]['risorse_tecniche'][rt]['id']) > 0 and letture2['schede_lavoro'][0]['risorse_umane'][ru]['ora_inizio'] == '000000' and letture2['schede_lavoro'][0]['risorse_umane'][ru]['ora_fine'] == '000000':
                        tipo_inserimento_rt.append(1)
                    else:
                        tipo_inserimento_rt.append(2)
                    
                    rt=rt+1
                    
                ru=0
                while ru < len(letture2['schede_lavoro'][0]['risorse_umane']):
                    logger.debug('Deattagli risorsa umana {0} : {1}'.format(
                        ru, 
                        letture2['schede_lavoro'][0]['risorse_umane'][ru])
                    )
                    id_ru.append(letture2['schede_lavoro'][0]['risorse_umane'][ru]['id'])
                    progr_ru.append(letture2['schede_lavoro'][0]['risorse_umane'][ru]['id_progressivo'])
                    flg_auti.append(letture2['schede_lavoro'][0]['risorse_umane'][ru]['flg_autista'])
                    ru_progr_rt.append(letture2['schede_lavoro'][0]['risorse_umane'][ru]['id_progr_ristec'])
                    mansioni.append(letture2['schede_lavoro'][0]['risorse_umane'][ru]['id_mansione'])
                    # controlllo il tipo inserimento presente
                    if int(letture2['schede_lavoro'][0]['risorse_umane'][ru]['id']) == 0: 
                        tipo_inserimento_ru.append(0)
                    elif int(letture2['schede_lavoro'][0]['risorse_umane'][ru]['id']) > 0 and letture2['schede_lavoro'][0]['risorse_umane'][ru]['ora_inizio'] == '000000' and letture2['schede_lavoro'][0]['risorse_umane'][ru]['ora_fine'] == '000000':
                        tipo_inserimento_ru.append(1)
                    else:
                        tipo_inserimento_ru.append(2)
                    
                    ru=ru+1
                
                logger.info('La scheda ha {0} risorse tecniche e {1} risorse umane'.format(
                        len(letture2['schede_lavoro'][0]['risorse_tecniche']),
                        len(letture2['schede_lavoro'][0]['risorse_umane'])))
                
                
                # risorse tecniche e umane vanno modificate in questo modo:
                # se c'è una sola risorsa tecnica, va modificata 
                logger.info('La scheda ha le seguenti risorse tecniche: {0}'.format(id_rt))
                logger.info('La scheda ha le seguenti risorse umane: {0}'.format(id_ru))
                logger.info('FLG autista: {0}'.format(flg_auti))
                logger.info('Mansioni: {0}'.format(mansioni))
                logger.info('Tipi inserimento: {0}'.format(tipo_inserimento_ru))
                
                
                
                # ora faccio un ciclo sulle registrazioni per quel percorso
                
                try:
                    curr_c.execute(query_registrazioni_percorso, (cod_percorso, datalav))
                    registrazioni_percorso=curr_c.fetchall()
                except Exception as e:
                    logger.error(query_registrazioni_percorso)
                    logger.error(e)
                
                
                for registrazione in registrazioni_percorso:
                    codice_badge=registrazione[0]
                    id_ekovision=registrazione[1]
                    cognome_ru=registrazione[2]
                    nome_ru=registrazione[3]
                    id_percorso=registrazione[4]
                    data_percorso=registrazione[5]
                    id_qualifica=registrazione[6]
                    id_mansione=registrazione[7]
                    sportello=registrazione[8]
                    datainsert_registrazione=registrazione[9]
                    
                    
                    
                    logger.info('Elaboro registrazione di {0} {1} con badge = {2}, cod_eko = {6} e qualifica {3} per il percorso {4} del {5}'.format(
                        nome_ru, cognome_ru, codice_badge, id_qualifica, id_percorso, data_percorso, id_ekovision
                    ))
                
                    if sportello is not None:
                        logger.info('Dovrei inserire la risorsa tecnica con id {0}'.format(sportello))
                        if sportello in id_rt:
                            logger.info('La scheda ha già la risorsa tecnica con id {0} con indice {1}'.format(sportello, id_rt.index(sportello)))
                            # prendo il progressivo della risorsa tecnica da usare per inserire la risorsa umana
                            progressivo_tmp = progr_rt[id_rt.index(sportello)]
                            logger.info('Il progressivo da usare per inserire la risorsa umana è {0}'.format(progressivo_tmp))
                        else:
                            # non c'è la risorsa tecnica da inserire, devo inserirla io
                            # caso 1 solo mezzo
                            if len(letture2['schede_lavoro'][0]['risorse_tecniche'])==1:
                                logger.info('La scheda ha una sola risorsa tecnica, va modificata')
                                # se id = 0 signigica che è una risorsa tecnica non è statadefinita  va modificata la prima risorsa tecnica
                                insert_update_rt = 0
                                if letture2['schede_lavoro'][0]['risorse_tecniche'][0]['id'] == 0 :
                                    insert_update_rt = 1    
                                elif letture2['schede_lavoro'][0]['risorse_tecniche'][0]['id'] > 0 and tipo_inserimento_rt[0] == 1:
                                    logger.error('La scheda ha una risorsa tecnica predefinita, la aggiorno con id {0} da totem'.format(sportello))
                                    letture2['schede_lavoro'][0]['risorse_tecniche'][0]['id']=sportello 
                                    insert_update_rt = 1     
                                # se non totem inviare una mail ????
                                else:
                                    testo_mail= f'''La scheda di lavoro con id {id_scheda} (codice percorso {cod_percorso} - {letture2['schede_lavoro'][0]['descr_scheda_lav']} 
                                    del {datalav}) ha una risorsa tecnica con targa {letture2['schede_lavoro'][0]['risorse_tecniche'][0]['targa']} 
                                    impostata manualmente su Ekovision, ma dalle info del totem dovrebbe essere modificata con sportello {sportello}.''' 
                                    warning_message_mail(testo_mail, 'roberto.marzochi@amiu.genova.it', os.path.basename(__file__), logger, 'Scheda di lavoro con risorsa tecnica da modificare')
                                
                                if insert_update_rt == 1 :   
                                    letture2['schede_lavoro'][0]['risorse_tecniche'][0]['id']=sportello
                                    letture2['schede_lavoro'][0]['risorse_tecniche'][0]['data_inizio']=letture2['schede_lavoro'][0]['servizi'][0]['data_inizio']
                                    letture2['schede_lavoro'][0]['risorse_tecniche'][0]['ora_inizio']=letture2['schede_lavoro'][0]['servizi'][0]['ora_inizio']
                                    letture2['schede_lavoro'][0]['risorse_tecniche'][0]['data_fine']=letture2['schede_lavoro'][0]['servizi'][0]['data_fine']
                                    letture2['schede_lavoro'][0]['risorse_tecniche'][0]['ora_fine']=letture2['schede_lavoro'][0]['servizi'][0]['ora_fine']     
                            elif len(letture2['schede_lavoro'][0]['risorse_tecniche'])==0:
                                logger.info('La scheda non ha risorse tecniche la aggiungo')
                                #letture2['schede_lavoro'][0]['risorse_tecniche'][0]['id']=sportello 
                                # TO DO MAIL
                                
                            elif len(letture2['schede_lavoro'][0]['risorse_tecniche'])>1:
                                # TODO ancora tutto da gestire
                                
                                # controllo se c'è id = 0 e se c'è modifico il primo 
                                logger.info('La scheda ha più di una risorsa tecnica, va modificata la prima con id=0')
                                rtt=0
                                while rtt < len(letture2['schede_lavoro'][0]['risorse_tecniche']):
                                    if letture2['schede_lavoro'][0]['risorse_tecniche'][rtt]['id'] == 0:
                                        letture2['schede_lavoro'][0]['risorse_tecniche'][rtt]['id']=rt  
                                        break
                                    rtt=rtt+1


                    if id_ekovision in id_ru:
                        logger.info('La scheda ha già la risorsa umana con id {0} con indice {1}'.format(id_ekovision, id_ru.index(id_ekovision)))
                        # se su Ekovision fosse definito come autista, ma su totem no tolgo il flag autista
                        if flg_auti[id_ru.index(id_ekovision)] == 1 and sportello is None:
                            logger.info('La risorsa umana id {0} è autista su Ekovision ma va tolto'.format(id_ekovision))
                            letture2['schede_lavoro'][0]['risorse_umane'][id_ru.index(id_ekovision)]['flg_autista']=0
                        elif flg_auti[id_ru.index(id_ekovision)] == 0 and sportello is not None:
                            logger.info('La risorsa umana con id {0} non è autista su Ekovision, ma lo è dalle info del totem --> correggo'.format(id_ekovision))
                            letture2['schede_lavoro'][0]['risorse_umane'][id_ru.index(id_ekovision)]['flg_autista']=1
                    
                    else:
                        logger.info('La scheda non ha la risorsa umana con id {0}, va aggiunta'.format(id_ekovision))
                        if len(letture2['schede_lavoro'][0]['risorse_umane'])==0:
                            logger.info('La scheda non ha risorse umane, devo capire se posso aggiungerle')
                        else:
                            logger.info('La scheda ha già risorse umane')
                            ru=0
                            # primo giro 
                            check_inserimento=0
                            while ru < len(letture2['schede_lavoro'][0]['risorse_umane']):
                                if id_mansione == mansioni[ru] and tipo_inserimento_ru[ru] == 0:
                                    check_inserimento=1
                                    logger.info('Ho trovato una risorsa umana con mansione {0} e tipo inserimento 0, quindi posso inserirla'.format(id_mansione))
                                    letture2['schede_lavoro'][0]['risorse_umane'][ru]['id']=id_ekovision  
                                    letture2['schede_lavoro'][0]['risorse_umane'][ru]['cognome']=cognome_ru # non funziona ma forse non serve
                                    letture2['schede_lavoro'][0]['risorse_umane'][ru]['nome']=nome_ru # non funziona ma forse non serve
                                    letture2['schede_lavoro'][0]['risorse_umane'][ru]['data_inizio']=letture2['schede_lavoro'][0]['servizi'][0]['data_inizio']
                                    letture2['schede_lavoro'][0]['risorse_umane'][ru]['ora_inizio']=letture2['schede_lavoro'][0]['servizi'][0]['ora_inizio']
                                    letture2['schede_lavoro'][0]['risorse_umane'][ru]['data_fine']=letture2['schede_lavoro'][0]['servizi'][0]['data_fine']
                                    letture2['schede_lavoro'][0]['risorse_umane'][ru]['ora_fine']=letture2['schede_lavoro'][0]['servizi'][0]['ora_fine']
                                    
                                    # se c'è una risorsa tecnica con id da totem e questa risorsa umana è autista, metto il flag autista
                                    if sportello is not None:
                                        logger.info('C\'è una risorsa tecnica con id {0} da totem, quindi la risorsa umana con id {1} è autista --> metto flag autista'.format(sportello, id_ekovision))
                                        letture2['schede_lavoro'][0]['risorse_umane'][ru]['flg_autista']=1
                                    logger.debug(letture2['schede_lavoro'][0]['risorse_umane'][ru])
                                    break
                                ru=ru+1
                            # secondo giro 
                            if check_inserimento == 0:
                                logger.info(f'''Non ho trovato una risorsa umana con mansione {id_mansione} e tipo inserimento 0, 
                                            quindi cerco una risorsa umana con tipo inserimento 1 per sostituirla''')
                                ru=0
                                while ru < len(letture2['schede_lavoro'][0]['risorse_umane']):
                                    if id_mansione == mansioni[ru] and tipo_inserimento_ru[ru] == 1:
                                        logger.info('Ho trovato una risorsa umana con mansione {0} e tipo inserimento 1, quindi posso sostituirla con la risorsa da inserire'.format(id_mansione))
                                        check_inserimento=1
                                        letture2['schede_lavoro'][0]['risorse_umane'][ru]['id']=id_ekovision  
                                        # se c'è una risorsa tecnica con id da totem e questa risorsa umana è autista, metto il flag autista
                                        if sportello is not None:
                                            logger.info('C\'è una risorsa tecnica con id {0} da totem, quindi la risorsa umana con id {1} è autista --> metto flag autista'.format(sportello, id_ekovision))
                                            letture2['schede_lavoro'][0]['risorse_umane'][ru]['flg_autista']=1
                                        logger.debug(letture2['schede_lavoro'][0]['risorse_umane'][ru])
                                        break
                                    ru=ru+1

                            if check_inserimento == 0:
                                # devo renderlo più comprensibile questo messaggio di errore
                                warning_msg = 'Non sono riuscito ad inserire la risorsa umana con id {0} con mansione {1} nella scheda {2}'.format(
                                    id_ekovision, 
                                    id_mansione, 
                                    id_scheda)
                
                
                
                exit()
                logger.info('Provo a salvare nuovamente la scheda')
                #logger.info(letture2)
                
                guid = uuid.uuid4()
                params2={'obj':'schede_lavoro',
                        'act' : 'w',
                        'ruid': '{}'.format(str(guid)),
                        'json': json.dumps(letture2, ensure_ascii=False).encode('utf-8')
                        }
                #exit()
                response2 = requests.post(eko_url_test, params=params2, data=auth_data_eko, headers=headers)
                result2 = response2.json()
                if result2['status']=='error':
                    logger.error('Id_scheda = {}'.format(id_scheda))
                    logger.error(result2)
                else :
                    logger.info(result2['status'])





    # check se c_handller contiene almeno una riga 
    error_log_mail(errorfile, 'assterritorio@amiu.genova.it', os.path.basename(__file__), logger)
    logger.info("chiudo le connessioni in maniera definitiva")
    
    curr_c.close()
    #currc1.close()
    conn_c.close()
    
    

if __name__ == "__main__":
    main()      