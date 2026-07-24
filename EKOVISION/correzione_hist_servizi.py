#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2026
# Roberto Marzocchi, Roberta Fagandini

'''
Da SIT elenco delle schede ekovision in cui lo stesso codice percorso è stato usato 2 volte nello stesso giorno
fino al 01/04/2026

- correggo hist_servizi su UO
- per sicurezza correggo anche consunt.persone su SIT

'''

#from msilib import type_short
import os, sys, re  # ,shutil,glob

import inspect, os.path
#import getopt  # per gestire gli input

#import pymssql

from datetime import date, datetime, timedelta


import psycopg2

import cx_Oracle

currentdir = os.path.dirname(os.path.realpath(__file__))
parentdir = os.path.dirname(currentdir)
sys.path.append(parentdir)
from credenziali import *


import requests
from requests.exceptions import HTTPError

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




def get_id_ut_percorso(curr, cod_percorso, data_esecuzione, logger):
    """
    Ritorna la lista con id_ut responsabile per un percorso attivo.
    Se non trova nulla, restituisce lista vuota.
    """
    query = '''
        SELECT u.id_zona 
        FROM anagrafe_percorsi.percorsi_ut pu
        join anagrafe_percorsi.cons_mapping_uo cmu on cmu.id_uo = pu.id_ut 
        join topo.ut u on u.id_ut = cmu.id_uo_sit  
        WHERE pu.cod_percorso = %s
        /*AND (responsabile = 'S' or pu.rimessa ='S')*/
        AND to_date(%s, 'YYYYMMDD') BETWEEN pu.data_attivazione AND pu.data_disattivazione
    '''
    
    # seconda query senza data esecuzione per verificare se il percorso è attivo o no
    query2 = '''
        SELECT u.id_zona 
        FROM anagrafe_percorsi.percorsi_ut pu
        join anagrafe_percorsi.cons_mapping_uo cmu on cmu.id_uo = pu.id_ut 
        join topo.ut u on u.id_ut = cmu.id_uo_sit  
        WHERE pu.cod_percorso = %s
    '''
    try:
        curr.execute(query, (cod_percorso, data_esecuzione))
        results = curr.fetchall()
        if len(results) == 0:
            messaggio_warning=f'''Non trovato id_ut per percorso {cod_percorso} alla data {data_esecuzione}.
            Probabilmente il percorso non è attivo alla data di esecuzione prevista (scheda di percorso stagionale generata per baco di ekovision)'''
            logger.warning(messaggio_warning)
            warning_message_mail(messaggio_warning, 'assterritorio@amiu.genova.it', os.path.basename(__file__), logger)

            #curr.execute(query2, (cod_percorso,))
            #results = curr.fetchall()
        return [r[0] for r in results]
    except Exception as e:
        logger.error("Errore eseguendo get_id_ut_percorso:")
        logger.error(query)
        logger.error(e)
        return []
    
    
    
# query select schede
select_schede = '''with schede_multiple as (
	select ce.codice_servizio_pred, ce.data_esecuzione_prevista
	from treg_eko.consunt_ekovision ce
	group by  ce.codice_servizio_pred, ce.data_esecuzione_prevista
	having count(distinct ce.id_scheda) > 1
)
select distinct id_scheda from treg_eko.consunt_ekovision ce
join schede_multiple sm on ce.codice_servizio_pred = sm.codice_servizio_pred 
	and ce.data_esecuzione_prevista = sm.data_esecuzione_prevista 
where ce.data_esecuzione_prevista < '20260402'
	order by id_scheda'''
 


# query select schede doppie da UO

select_schede_uo= '''with schede_multiple as (
	select ce.codice_serv_pred, ce.data_esecuzione_prevista
	from SCHEDE_ESEGUITE_EKOVISION  ce
	group by  ce.codice_serv_pred, ce.data_esecuzione_prevista
	having count(distinct ce.id_scheda) > 1
)
select distinct id_scheda from SCHEDE_ESEGUITE_EKOVISION ce
join schede_multiple sm on ce.codice_serv_pred = sm.codice_serv_pred 
	and ce.data_esecuzione_prevista = sm.data_esecuzione_prevista 
where ce.data_esecuzione_prevista >= '20260101' and ce.data_esecuzione_prevista < '20260110'
	order by id_scheda'''




# schede con orari anomali
"""
select_schede_uo= '''SELECT ID_SCHEDA_EKOVISION FROM hist_servizi WHERE DURATA > 480
AND ID_SCHEDA_EKOVISION IS NOT null'''
"""


""""
# correzione di Vento domenico (caso di omonimia)
select_schede_uo= '''SELECT ID_SCHEDA_EKOVISION FROM HIST_SERVIZI hs 
WHERE hs.COD_DIPENDENTE IN ('08628_1', '08627_1')
AND hs.ID_SCHEDA_EKOVISION IS NOT null'''

# correzione di Ferrando Paolo (caso di omonimia)
select_schede_uo= '''SELECT ID_SCHEDA_EKOVISION FROM HIST_SERVIZI hs 
WHERE hs.COD_DIPENDENTE IN ('03660_1', '07477_1')
AND hs.ID_SCHEDA_EKOVISION IS NOT null'''
"""


select_matricola = '''
SELECT tape.COD_MATLIBROMAT AS MATRICOLA
/*, pe.* */
FROM PERSONALE_EKOVISION pe 
LEFT JOIN T_ANAGR_PERS_EKOVISION tape 
	ON (trim(tape.NOMINATIVO) = trim(pe.COGNOME) || ' ' || trim (pe.NOME) 
 and to_date(pe.DT_NASCITA, 'YYYYMMDD') = tape.DATA_NASCITA)
		OR tape.COD_MATLIBROMAT = pe.MATRICOLA
WHERE  pe.ID_EKOVISION = :id_ekovision AND to_date(:data_inizio , 'YYYYMMDD') 
BETWEEN tape.DTA_INIZIO AND tape.dta_fine
'''
    
query_id_ser_per_uo='''SELECT ID_SER_PER_UO , ID_TURNO, ID_UO, ID_SERVIZIO 
    FROM ANAGR_SER_PER_UO aspu WHERE ID_PERCORSO LIKE :c1
    AND to_date(:c2, 'YYYYMMDD') BETWEEN DTA_ATTIVAZIONE AND DTA_DISATTIVAZIONE '''




query_delete='''DELETE FROM UNIOPE.HIST_SERVIZI 
        WHERE DTA_SERVIZIO=to_date(:h1,'YYYYMMDD') AND 
        ID_SER_PER_UO=:h2 and 
        (ID_SCHEDA_EKOVISION=:h3 or ID_SCHEDA_EKOVISION is null)'''
                                                
query_insert_hs='''INSERT INTO UNIOPE.HIST_SERVIZI 
        (DTA_SERVIZIO, ID_SER_PER_UO, COD_DIPENDENTE,
        PROG_SERVIZIO, ID_UO_LAVORO, DURATA,
        ID_TURNO, ID_SCHEDA_EKOVISION) 
        VALUES(to_date(:h1,'YYYYMMDD'), :h2, :h3,
        1 , :h4, :h5,
        :h6, :h7)'''       
        
       
select_sit = '''select cod_dipendente, durata
from consunt.persone where id_scheda_ekovision = %s''' 
        
        
                                                 
def main():
    
    
    
    # logger 
    
    
    filename = inspect.getframeinfo(inspect.currentframe()).filename
    #path = os.path.dirname(os.path.abspath(filename))
    path1 = os.path.dirname(os.path.dirname(os.path.abspath(filename)))
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
    
    
    logger.info('Il PID corrente è {0}'.format(os.getpid()))
            


    # credenziali per connessione a EKOVISION
    headers = {'Content-Type': 'application/x-www-form-urlencoded'}
    auth_data_eko={'user': eko_user, 'password': eko_pass, 'o2asp' :  eko_o2asp}


    # Mi connetto a SIT (PostgreSQL) per poi recuperare le mail
    nome_db=db
    logger.info('Connessione al db {}'.format(nome_db))
    conn = psycopg2.connect(dbname=nome_db,
                        port=port,
                        user=user,
                        password=pwd,
                        host=host)


    curr = conn.cursor()
    
    
    
    # Mi connetto al DB oracle UO
    cx_Oracle.init_oracle_client(percorso_oracle) # necessario configurare il client oracle correttamente
    #cx_Oracle.init_oracle_client() # necessario configurare il client oracle correttamente
    parametri_con='{}/{}@//{}:{}/{}'.format(user_uo,pwd_uo, host_uo,port_uo,service_uo)
    logger.debug(parametri_con)
    con = cx_Oracle.connect(parametri_con)
    logger.info("Versione ORACLE: {}".format(con.version))
    
    cur = con.cursor()
    
    cur.execute("ALTER SESSION SET NLS_DATE_FORMAT = 'YYYYMMDD'")
    cur.execute("ALTER SESSION SET NLS_LANGUAGE = 'ITALIAN'")
    cur.execute("ALTER SESSION SET NLS_TERRITORY = 'ITALY'")
    
    
    debug=1
    
    if debug == 0:

        logger.info('Eseguo query per recuperare le schede con stesso codice percorso e stessa data pianificazione')
        
        """
        try:
            curr.execute(select_schede)
            schede = curr.fetchall()
        except Exception as e:
            logger.error(select_schede)
            logger.error(e)        
        """
        try:
            cur.execute(select_schede_uo)
            schede = cur.fetchall()
        except Exception as e:
            logger.error(select_schede_uo)
            logger.error(e)
            
    else:
        # scheda usata per test 
        schede = [(923149,)]
    
    
    
    
    for s in schede:  
    
        logger.info(f'Provo a leggere i dettagli della scheda {s[0]}')
    
    
        
                                                
                                                
                                                
        
            
        
    
        params2={'obj':'schede_lavoro',
                'act' : 'r',
                'id': '{}'.format(int(s[0])),
                'flg_esponi_consunt' : 1
                }
    
        response2 = requests.post(eko_url, params=params2, data=auth_data_eko, headers=headers)
        #letture2 = response2.json()
        letture2 = response2.json()


        
        


        if letture2['status'] == 'error':
            logger.warning(letture2)
        else:
            
            # PROCEDO ALLA CANCELLAZIONE DEL PREGRESSO
                
            # RECUPERO DATA PERCORSO
            data_percorso=letture2["schede_lavoro"][0]["data_inizio_lav"]
            logger.debug(f'Data percorso: {data_percorso}')
    
    
    
            # devo trovare id_ser_per_uo       
            cod_percorso=letture2["schede_lavoro"][0]["servizi"][0]['cod_modello']
            logger.debug(f'Codice percorso: {cod_percorso}')                        
                                    
            try:
                cur.execute(query_id_ser_per_uo, (cod_percorso, data_percorso,))
                ii_ss=cur.fetchall()
            except Exception as e:
                logger.error(query_id_ser_per_uo)
                logger.error(e)
                check_lettura+=1                                            

            id_rimessa=''
            id_ut=''
            for ispu in ii_ss:
                id_ser_per_uo=ispu[0]
                id_turno=ispu[1]
                id_servizio=ispu[3]
                if int(ispu[2])==16 or int(ispu[2])==17:
                    id_rimessa=ispu[2]
                else:
                    id_ut=ispu[2]
        
            logger.debug(f'id_rimessa: {id_rimessa}')
            logger.debug(f'id_ut: {id_ut}')
            logger.debug(f'id_turno: {id_turno}')
            logger.debug(f'id_servizio: {id_servizio}')
            
            
            logger.info(f'Cancello eventuali pregressi in hist_servizi per la scheda {s[0]} e data percorso {data_percorso}')
            try:
                cur.execute(query_delete, (data_percorso, id_ser_per_uo, s[0]))
            except Exception as e:
                logger.error(query_delete)
                logger.error('1:{}, 2:{}, 3:{}'.format(data_percorso,             
                                                id_ser_per_uo, s[0]))
                logger.error(e)
            
            
            
            # nel caso in cui la scheda sia stata effettuata cerco chi l'abbia eseguita
            if letture2["schede_lavoro"][0]["servizi"][0]['flg_segn_srv_non_effett']=="0":
        
        
                # checje se tramite WS riesco a recuperare le persone che hanno lavorato sulla scheda, se non riesco a recuperarle non faccio la correzione di hist_servizi per evitare di perdere dati corretti su persone che non riesco ad identificare, ma mando una mail di warning per segnalare il problema
                check_persone_ws=0
                
                p = 0
                
                while p< len(letture2["schede_lavoro"][0]["risorse_umane"]):
                    logger.info(f'Provo a leggere i dettagli della risorsa umana {p} della scheda {s[0]}')
                    #logger.debug(letture2["schede_lavoro"][0]["risorse_umane"][p]) 
                    
                   
                    
                    id_persona_ekovision = int(letture2["schede_lavoro"][0]["risorse_umane"][p]['id'])
                    if id_persona_ekovision  > 0:
                        id_mansione = letture2["schede_lavoro"][0]["risorse_umane"][p]['id_mansione']
                        logger.debug(f'id mansione: {id_mansione}')
                        
                        do_ini_risorsa=datetime.strptime(f'{letture2["schede_lavoro"][0]["risorse_umane"][p]["data_inizio"]} {letture2["schede_lavoro"][0]["risorse_umane"][p]["ora_inizio"]}', 
                                                                        '%Y%m%d %H%M%S')
                        
                        data_inizio=datetime.strptime(f'{letture2["schede_lavoro"][0]["risorse_umane"][p]["data_inizio"]}', '%Y%m%d')
                        ora_inizio=letture2["schede_lavoro"][0]["risorse_umane"][p]["ora_inizio"]
                        
                        
                                               
                        do_fine_risorsa=datetime.strptime(f'{letture2["schede_lavoro"][0]["risorse_umane"][p]["data_fine"]} {letture2["schede_lavoro"][0]["risorse_umane"][p]["ora_fine"]}', 
                                                                        '%Y%m%d %H%M%S')
                        
                        data_fine=datetime.strptime(f'{letture2["schede_lavoro"][0]["risorse_umane"][p]["data_fine"]}', '%Y%m%d')
                        ora_fine=letture2["schede_lavoro"][0]["risorse_umane"][p]["ora_fine"]
                        
                        if do_fine_risorsa < do_ini_risorsa:
                            logger.warning(f'La data di fine risorsa è precedente alla data di inizio risorsa per la risorsa umana {p} della scheda {s[0]}. Probabilmente c\'è un errore nei dati di ekovision, controllo i valori:')
                            
                            do_fine_risorsa= do_fine_risorsa + timedelta(days=1)
                        
                        # correzione stronzate WS di Ekovision 
                        logger.debug(str(do_ini_risorsa.hour).rjust(2,'0'))
                        logger.debug(str(do_fine_risorsa.hour).rjust(2,'0'))
                        if str(do_ini_risorsa.hour).rjust(2,'0')+str(do_ini_risorsa.minute).rjust(2,'0') < str(do_fine_risorsa.hour).rjust(2,'0')+str(do_fine_risorsa.minute).rjust(2,'0') and data_inizio < data_fine:
                            logger.warning(f'''c'è qualche casino sulle date che correggo a mano''')
                            do_fine_risorsa= do_fine_risorsa - timedelta(days=1)
                        
                        # calcolo la durata in minuti della risorsa umana sulla scheda
                        logger.debug(f'Data inizio risorsa: {do_ini_risorsa}')
                        logger.debug(f'Data fine risorsa: {do_fine_risorsa}')
                        durata_risorsa=(do_fine_risorsa - do_ini_risorsa).total_seconds()/60
                        
                        
                        logger.debug(f'Durata risorsa umana con id {id_persona_ekovision} della scheda {s[0]} in minuti: {durata_risorsa}')
                        
                        # devo recuperare codice persona 
                        check_persona=0
                        try:
                            cur.execute(select_matricola, (id_persona_ekovision, data_percorso,))
                            matricola = cur.fetchall()[0][0]
                        except Exception as e:
                            logger.warning(f'''Non riesco a trovare la persona con id {id_persona_ekovision} su UO per la scheda {s[0]} e data percorso {data_percorso}.''')
                            error_message1 = f'''Per la scheda {s[0]} non trovata non trovata persona su Ekovision. Potrebbe essere persona che a fine 2025 non lavorava più in azienda 
                                         oppure nome con caratteri speciali'''
                            error_message2 = f'Id persona ekovision: {id_persona_ekovision}, data percorso: {data_percorso}'
                            check_persona = 1
                            check_persone_ws = 1
                            #logger.error(select_matricola)
                            #logger.error(e)
                        
                        # se ho trovato la persona da WS proseguo
                        if check_persona==0:
                            logger.debug(f'Matricola: {matricola}')   
                            cod_dipendente = str(matricola).rjust(5, '0')+'_1'
                            logger.debug(f'Codice dipendente: {cod_dipendente}')
                            
                            
                            
                            
                            if id_rimessa!='' and id_mansione==33:
                                id_ut_ok=id_rimessa
                            elif id_ut != '' and id_mansione!=33 :
                                id_ut_ok=id_ut
                            elif id_ut=='' and id_rimessa!='':
                                id_ut_ok=id_rimessa
                            elif id_ut!='' and id_rimessa=='':
                                id_ut_ok=id_ut       
                            
                            logger.debug(f'id_ut_ok: {id_ut_ok}')
                        
                        
                        # se non avessi ancora trovato la persona la provo a cercare su SIT 
                        
                        
                        
                                        
                     
                        if check_persona==0:

                            
                            try:
                                cur.execute(query_insert_hs, (data_percorso, 
                                                    id_ser_per_uo, cod_dipendente,
                                                    id_ut_ok, durata_risorsa, 
                                                    id_turno, s[0]))

                            except Exception as e:
                                logger.error(query_insert_hs)
                                logger.error(e)
                                messaggio= f'''
                                        Problema con INSERT INTO UNIOPE.HIST_SERVIZI 
                                        Id scheda: {s[0]}, 
                                        Data percorso: {data_percorso},
                                        cod_servizio: {cod_percorso})'''
                                try: 
                                    id_ser_per_uo=int(id_ser_per_uo)
                                except:
                                    logger.error('id_ser_per_uo non è un intero: {}'.format(id_ser_per_uo)) 
                                try: 
                                    logger.error('cod_dipendente: {}'.format(cod_dipendente))
                                except:
                                    logger.error('Non riesco a scrivere cod_dipendente')
                                try: 
                                    logger.error('Durata: {}'.format(durata_risorsa))
                                except:
                                    logger.error('Non riesco a scrivere durata')
                                try:                                                     
                                    logger.error('id_ut_ok: {}'.format(id_ut_ok))
                                except: 
                                    logger.error('Non riesco a scrivere id_ut_ok')
                                try: 
                                    logger.error('id_turno: {}'.format(id_turno))
                                except:
                                    logger.error('Non riesco a scrivere id_turno')
                                                                                

                            
                                logger.error(messaggio)
                                # mando mail e mi fermo
                                warning_message_mail(messaggio, 'assterritorio@amiu.genova.it', os.path.basename(__file__), logger)


                            
                            query_insert_sit='''INSERT INTO consunt.persone
                                (id_scheda_ekovision, cod_dipendente, durata, filename)
                                VALUES(%s, %s, %s, 'From WS') 
                                ON CONFLICT (id_scheda_ekovision, cod_dipendente) 
                                DO UPDATE  SET durata=EXCLUDED.durata'''
                            
                            try:
                                curr.execute(query_insert_sit, (s[0], cod_dipendente.rjust(7, '0'), durata_risorsa))
                            except Exception as e:
                                logger.error(query_insert_sit)
                                logger.error('scheda:{}, persona:{}, durata:{}, filename:{}'.format(s[0], cod_dipendente, durata_risorsa))
                                logger.error(e)
                            # faccio commit su entrambi i db dopo ogni risorsa umana per evitare di perdere dati in caso di errori su risorse successive
                            
                        else:
                            logger.warning(f'''Non correggo hist_servizi per la risorsa umana {p} della scheda {s[0]} 
                                           perchè non riesco a trovare la persona sul db della UO ''')  

                            # non faccio commit quindi nemmeno delete


                    p+=1
                    
                correzione_con_sit = 0
                # se check_persone_ws non fosse 0 non faccio il commit
                if check_persone_ws==0:
                    conn.commit()
                    con.commit() 
                else : 
                    logger.warning(f'''Non faccio commit su nessuno dei due db per la scheda {s[0]} 
                                    perchè non riesco a recuperare le persone da WS, 
                                    in questo modo evito di perdere dati corretti su persone che non riesco ad identificare''')
                    logger.info('Faccio rollback su entrambi i db per la scheda {}'.format(s[0])
                                )
                    con.rollback()
                    conn.rollback()
                    
                    
                    
                    
                    logger.info(f'''Avendo fatto rollback provo di nuovo a cancellare 
                                eventuali pregressi in hist_servizi per la scheda {s[0]} e data percorso {data_percorso}''')
                    try:
                        cur.execute(query_delete, (data_percorso, id_ser_per_uo, s[0]))
                    except Exception as e:
                        logger.error(query_delete)
                        logger.error('1:{}, 2:{}, 3:{}'.format(data_percorso,             
                                                        id_ser_per_uo, s[0]))
                        logger.error(e)

                    # provo a cercare la persona su SIT
                    logger.info(f'''Provo a cercare la persona su SIT per la scheda {s[0]}''')
                    try:
                        curr.execute(select_sit, (s[0],))
                        persone_sit = curr.fetchall()
                    except Exception as e:
                        logger.error(select_sit)
                        logger.error(e)
                    
                    
                    logger.info(f'''Trovo {len(persone_sit)} persone su SIT per la scheda {s[0]}''')
                    #exit()
                    for ps in persone_sit:
                        durata_risorsa= ps[1]
                        cod_dipendente=ps[0]
                        
                        
                        try:
                            cur.execute(query_insert_hs, (data_percorso, 
                                                id_ser_per_uo, cod_dipendente,
                                                id_ut, durata_risorsa, 
                                                id_turno, s[0]))

                            con.commit()
                            logger.info(f'''Correzione riuscita inserendo la persona trovata su SIT per la scheda {s[0]}''')
                            correzione_con_sit = 1
                        except Exception as e:
                            logger.error(query_insert_hs)
                            logger.error(e)
                            messaggio= f'''
                                    Problema con INSERT INTO UNIOPE.HIST_SERVIZI 
                                    Id scheda: {s[0]}, 
                                    Data percorso: {data_percorso},
                                    cod_servizio: {cod_percorso})'''
                            try: 
                                id_ser_per_uo=int(id_ser_per_uo)
                            except:
                                logger.error('id_ser_per_uo non è un intero: {}'.format(id_ser_per_uo)) 
                            try: 
                                logger.error('cod_dipendente: {}'.format(cod_dipendente))
                            except:
                                logger.error('Non riesco a scrivere cod_dipendente')
                            try: 
                                logger.error('Durata: {}'.format(durata_risorsa))
                            except:
                                logger.error('Non riesco a scrivere durata')
                            try:                                                     
                                logger.error('id_ut: {}'.format(id_ut))
                            except: 
                                logger.error('Non riesco a scrivere id_ut')
                            try: 
                                logger.error('id_turno: {}'.format(id_turno))
                            except:
                                logger.error('Non riesco a scrivere id_turno')                                                

                        
                            logger.error(messaggio)
                            # mando mail e mi fermo
                            warning_message_mail(messaggio, 'assterritorio@amiu.genova.it', os.path.basename(__file__), logger)
                        
                        
                
                if check_persona==1 and correzione_con_sit==0:
                    logger.error(error_message1)
                    logger.error(error_message2)
            
            
            elif letture2["schede_lavoro"][0]["servizi"][0]['flg_segn_srv_non_effett']=="1":
                logger.info(f'La scheda {s[0]} è già stata segnata come non effettuata, quindi non correggo hist_servizi e consunt.persone')

                
                
                
            else:
                logger.error(f'Valore non previsto per flg_segn_srv_non_effett: {letture2["schede_lavoro"][0]["servizi"][0]["flg_segn_srv_non_effett"]} per la scheda {s[0]}')
                warning_message_mail(f'Valore non previsto per flg_segn_srv_non_effett: {letture2["schede_lavoro"][0]["servizi"][0]["flg_segn_srv_non_effett"]} per la scheda {s[0]}')
                   

        #exit()
    
    
    # check se c_handller contiene almeno una riga 
    error_log_mail(errorfile, 'assterritorio@amiu.genova.it', os.path.basename(__file__), logger)
    
    
    logger.info("chiudo le connessioni in maniera definitiva")

    
    cur.close()
    con.close()
    curr.close()
    conn.close()
    




if __name__ == "__main__":
    main()  