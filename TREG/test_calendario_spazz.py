#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2023
# Roberto Marzocchi

'''
Scopo dello script è lavorare giorno per giorno e individuare eventuali anomalie su Ekovisiona partire dalla programmazione SIT (su percorsi spazzamento)


PUNTI DI PARTENZA: 

1) query che fa union di 3 viste:
    ▪ anagrafe_percorsi.v_percorsi_elementi_tratti
    ▪ anagrafe_percorsi.v_percorsi_elementi_tratti_ovs (OVS = Old Version SIT)
    ▪ anagrafe_percorsi.mv_percorsi_elementi_tratti_dismessi 
- join con aste, via per recuperare informazioni sulla via 



2) periodo di attività del percorso, per i percorsi stagionali o dismessi 
l'elemento / elemento_asta_percorso non sono eliminati quindi nella query (tabella anagrafe_percorsi.elenco_percorsi)

3) turno previsto (tabella anagrafe_percorsi.elenco_percorsi)

4) servizio da inviare ad ARERA (tabella anagrafe_percorsi.anagrafe_tipo)


VERIFICHE DA FARE
Devo usare i WS di Ekovision:
    - elenco schede lavoro entrando con cod_percorso, data controllo che ci sia almeno 1 scheda
        --> percorsi_spazz_non_presenti.txt
        --> percorsi_spazz_doppi.txt 
    - entro con id_scheda e devo verificare i tratti 
        --> percorsi_tratti_non_trovati.txt: tutti i tratti di SIT devono essere in Ekovision
        --> percorsi_spazz_spunte_colorate.txt: i tratti di Ekovision non presenti su SIT dovrebbero individuar i percorsi con spunte blue e marroni


'''

#from msilib import type_short
import os, sys, re  # ,shutil,glob
import inspect

import requests
from requests.exceptions import HTTPError

import json


#import getopt  # per gestire gli input

#import pymssql

from datetime import date, datetime, timedelta

import locale

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


from tappa_prevista import *

from crea_dizionario_da_query import *



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



#variabile che specifica se devo fare test ekovision oppure no
test_ekovision=0
    
     

def main():
      


    
    logger.info('Il PID corrente è {0}'.format(os.getpid()))

    
    # Get today's date
    #presentday = datetime.now() # or presentday = datetime.today()
    oggi=datetime.today()
    oggi=oggi.replace(hour=0, minute=0, second=0, microsecond=0)
    oggi=date(oggi.year, oggi.month, oggi.day)
    #logging.debug('Oggi {}'.format(oggi))
    
    oggi_char=oggi.strftime('%Y%m%d')
    
    
    
    # credenziali WS Ekovision
    headers = {'Content-Type': 'application/x-www-form-urlencoded'}
    data={'user': eko_user, 
        'password': eko_pass,
        'o2asp' :  eko_o2asp
        }
    
    

    
    # connessione a SIT
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
    
    
    
    file_anomalie = open(f"{path}/output/tappe_spazzamenti_doppie.csv", "w")
    file_anomalie.write('cod_percorso; id_ekovision; pos; cod_tratto\n')
    
    # creo 3 dizionari e una lista per verificare anomalie sui percorsi
        
    dict_percorsi_non_presenti={} # questo deve assolutamente essere nullo
    
    dict_percorsi_doppi={} # questi ci sono ma è utile attenzionarli    
    
    lista_percorsi_da_verificare=[] # questi sono quei percorsi con spunte blu/marroni su Eko che hanno delle componenti duplicate o in più rispetto alla situazione in quella data
    
    dict_tratti_non_trovati={} # questi sono componenti presenti su Ekovision che non dovrebbero esserci in quei soli percorsi che hanno stesso numero di componenti, quindi errore più grave

    
    dict_tratti_eko_sit={}
    
    
    query_tratti_eko_sit='''
        select * from etl.tratti_ekovision
        '''

    try:
        curr.execute(query_tratti_eko_sit)
        tratti_aste=curr.fetchall()
    except Exception as e:
        logger.error(query_tratti_eko_sit)
        logger.error(e)
    
    for ta in tratti_aste:
        dict_tratti_eko_sit[ta[0]]=ta[1]
    
    #logger.debug(dict_tratti_eko_sit.keys())
    #exit()
    
    
    # ciclo sui giorni a partire dall'ultima volta in cui ha girato
    curr.close()

    curr = conn.cursor()
    
    # cerco il giono da cui partire
    """
    query_first_day='''SELECT coalesce(max(data_last_calendar), to_date('20250101', 'YYYYMMDD')) as data_last_calendar
FROM treg_eko.last_import_treg_spazz; '''
    """
    query_first_day='''SELECT to_date('20250801', 'YYYYMMDD') as data_last_calendar
FROM treg_eko.last_import_treg_spazz; '''
    try:
        curr.execute(query_first_day)
        giorno_mese_anno=curr.fetchall()
    except Exception as e:
        check_error=1
        logger.error(query_first_day)
        logger.error(e)
    
    
    for gma in giorno_mese_anno:
        data_start=gma[0]
    logger.debug('{} era {}'.format(data_start, data_start.strftime('%A')))


    #########################################################################
    # IMPOSTO FINE CICLO
    
    # per arrivare ad oggi
    #fine_ciclo=oggi

    # per fermarmi prima di oggi 
    fine_ciclo = datetime.strptime('20250901', '%Y%m%d')
    fine_ciclo=date(fine_ciclo.year, fine_ciclo.month, fine_ciclo.day)
    #########################################################################
    
    
    while  data_start <= fine_ciclo:
        logger.info('Processo il giorno {}'.format(data_start))
        if data_start.isocalendar()[1]%2 == 1:
            check_s='D'
        else:
            check_s='P'

        logger.info('La settimana è {}'.format(check_s))
        
        query_elenco_percorsi_spazz='''
        select cod_percorso, versione_testata, fo.freq_binaria, freq_settimane, id_turno, at2.gestione_arera 
            from anagrafe_percorsi.elenco_percorsi ep 
            join anagrafe_percorsi.anagrafe_tipo at2 on at2.id = ep.id_tipo
            join etl.frequenze_ok fo on fo.cod_frequenza = ep.freq_testata 
            where %s between data_inizio_validita and (data_fine_validita - interval '1' day) 
            and gestione_arera = 't'
            and at2.id_famiglia in (2,3) /* consideriamo i soli servizi igiene */ 
            '''

        try:
            curr.execute(query_elenco_percorsi_spazz, (data_start,))
            elenco_percorsi=curr.fetchall()
        except Exception as e:
            check_error=1
            logger.error(query_elenco_percorsi_spazz)
            logger.error(e)

        # facciamo un dizionario con chiave cod_percorso  e valore turno
        dict_percorsi={}
       
        i=0
        for ep in elenco_percorsi:
            # cod percorso 0
            # versione_testata 1
            # freq_testata 2
            # freq_settimane 3
            # id_turno 4
            # at2.gestione_arera 5
            
            #logger.debug(ep[0])
            # 1 se prevista # - 1 se non prevista
            # check_s è la settimana del giorno (se P o D)
            # freq_settimane può 
            
            # !!!!!!!!!!!!!!!!!!!!!!!! DA AGGIUNGERE  CONDIZIONE SUL SERVIZIO ARERA  !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
            if tappa_prevista(data_start,  ep[2])==1 and (ep[3]=='T' or ep[3]==check_s): 
                dict_percorsi[ep[0]]=ep[4]
            
            '''
            i+=1
            if i>10:
                exit()
            '''
            
            
        
        
        #logger.debug(dict_percorsi)
        #exit()   
        # c è la chiave (codice turno)
        # t è il turno      
        for c, t in dict_percorsi.items():
            #logger.debug(c + ' : ' + str(t))
            
            
            # TEST EKOVISION (che a regime si potrà togliere????)
                        # con questo WS devo verificare se esiste la scheda di lavoro per quel giorno 
            params={'obj':'schede_lavoro',
                'act' : 'r',
                'sch_lav_data': data_start.strftime('%Y%m%d'),
                'cod_modello_srv': c,
                'flg_includi_eseguite': 1,
                'flg_includi_chiuse': 1
                }
            response = requests.post(eko_url, params=params, data=data, headers=headers)
            #response.json()
            #logger.debug(response.status_code)
            check=0
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
                logger.info(letture)
                
                #exit()
                
                id_scheda=[]
                # leggo tutte le schede di quel giorno
                if len(letture['schede_lavoro'])==0:
                    # questo sarebbe un problema 
                    dict_percorsi_non_presenti[c]=data_start
                elif len(letture['schede_lavoro'])>1:
                    # queste le attenzioniamo
                    dict_percorsi_doppi[c]=data_start
                    ss=0
                    while ss < len(letture['schede_lavoro']):
                        if int(letture['schede_lavoro'][ss]['id_scheda_lav']) not in id_scheda:
                            id_scheda.append(int(letture['schede_lavoro'][ss]['id_scheda_lav']))
                        ss+=1
                else:
                    id_scheda.append(int(letture['schede_lavoro'][0]['id_scheda_lav']))
            
            # ora devo verificare i tratti
            logger.debug('lista id_scheda = {}'.format(id_scheda))
            tratti_sit=[]
            tratti_Eko=[] # !! che da WS mi tiro fuori id_ekovision e non id_elemento
            
            
            # cerco quelle di SIT
            query_elementi_percorso='''
            select codice_modello_servizio as cod_percorso,
            fo.freq_binaria, 
            'OTH' as areaType, 
            codice as areaCode,
            tab.ripasso,
            aa.id_via as streetCode,
            v.nome as streetDescription, 
            c.cod_istat as istatCode, 
            min(tab.data_inizio) as data_inizio,
            max(tab.data_fine) as data_fine
            from 
            (
                SELECT codice_modello_servizio, ordine, objecy_type, 
            codice, quantita, lato_servizio, percent_trattamento,frequenza,
            ripasso, numero_passaggi, replace(replace(coalesce(nota,''),'DA PIAZZOLA',''),';', ' - ') as nota,
            codice_qualita, codice_tipo_servizio, data_inizio, coalesce(data_fine, '20991231') as data_fine, 
            id_asta_percorso, id_elemento_asta_percorso
                FROM anagrafe_percorsi.v_percorsi_elementi_tratti where data_inizio < coalesce(data_fine, '20991231')
                union 
                SELECT codice_modello_servizio, ordine, objecy_type, 
            codice, quantita, lato_servizio, percent_trattamento,frequenza,
            ripasso, numero_passaggi, replace(replace(coalesce(nota,''),'DA PIAZZOLA',''),';', ' - ') as nota,
            codice_qualita, codice_tipo_servizio, data_inizio, coalesce(data_fine, '20991231') as data_fine,
            id_asta_percorso, id_elemento_asta_percorso
                FROM anagrafe_percorsi.v_percorsi_elementi_tratti_ovs where data_inizio < coalesce(data_fine, '20991231')
                union 
                SELECT codice_modello_servizio, ordine, objecy_type, 
            codice, quantita, lato_servizio, percent_trattamento,frequenza,
            ripasso, numero_passaggi, replace(replace(coalesce(nota,''),'DA PIAZZOLA',''),';', ' - ') as nota,
            codice_qualita, codice_tipo_servizio, data_inizio, coalesce(data_fine, '20991231') as data_fine, 
            id_asta_percorso, id_elemento_asta_percorso
                FROM anagrafe_percorsi.mv_percorsi_elementi_tratti_dismessi where data_inizio < coalesce(data_fine, '20991231')
            ) tab
            left join (select id_asta, id_via from elem.aste
            union 
            select id_asta, id_via from history.aste) aa
                on aa.id_asta = tab.codice
            left join topo.vie v on v.id_via = aa.id_via 
            left join topo.comuni c on c.id_comune = v.id_comune 
            left join etl.frequenze_ok fo on fo.cod_frequenza = tab.frequenza
            where
            tab.data_fine > '20250101'
            and objecy_type = 'TRATTO'
            and codice_modello_servizio = %s
            and %s between tab.data_inizio and tab.data_fine
            group by codice_modello_servizio,
            fo.freq_binaria, 
            tab.codice,
            tab.ripasso,
            aa.id_via ,
            v.nome , 
            c.cod_istat
            '''
            
            try:
                curr.execute(query_elementi_percorso, (c,data_start.strftime('%Y%m%d')))
                elenco_elementi_percorso=curr.fetchall()
            except Exception as e:
                logger.error(query_elementi_percorso)
                logger.error(e)
            
            # popolo tratti_sit
            for eep in elenco_elementi_percorso:
                # verifico se in frequenza con la solita funzione
                if tappa_prevista(data_start,  eep[1])==1:
                    # questa va confrontata con i risultati del WS, le altre no
                    tratti_sit.append(eep[3])
                
            
            
            
            # ora cerco quelle di Ekovision
            for ids in id_scheda:
                # con questo WS devo verificare se esiste la scheda di lavoro per quel giorno 
                params={'obj':'schede_lavoro',
                    'act' : 'r',
                    'id': ids
                    }
                response = requests.post(eko_url, params=params, data=data, headers=headers)
                #response.json()
                #logger.debug(response.status_code)
                check=0
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
                    #logger.debug(letture)
                    ss=0
                    while ss < len(letture['schede_lavoro']):
                        trips=letture['schede_lavoro'][ss]['trips']
                        #logger.debug(trips)
                        # ciclo sulle aste 
                        tr=0
                        while tr < len(trips):
                            waypoints=letture['schede_lavoro'][ss]['trips'][tr]['waypoints']
                            wid=0
                            while wid < len(waypoints):
                                works=letture['schede_lavoro'][ss]['trips'][tr]['waypoints'][wid]['works'] 
                                # ciclo sugli elementi
                                cc=0
                                while cc < len(works):
                                    tratti_Eko.append(int(letture['schede_lavoro'][ss]['trips'][tr]['waypoints'][wid]['works'][cc]['id_object']))
                                    cc+=1
                                wid+=1
                            tr+=1
                        ss+=1    
                #exit()        
            
            
            #ora che abbiamo le 2 liste per quel percorso e quella data le dobbiamo confrontare
            lista_tratti_anomali=[]
            if len(tratti_Eko)>len(tratti_sit):
                """query_eko='''SELECT ces.COD_TRATTO, posizione 
            FROM CONSUNT_EKOVISION_SPAZZAMENTO ces 
            WHERE ces.ID_SCHEDA = :d1
            AND ces.RECORD_VALIDO = 'S'
            ORDER BY 2'''
                try:
                    cur.execute(query_eko, (id_scheda[0],))
                    cod_tratti=cur.fetchall()
                except Exception as e:
                    logger.error(query_elementi_percorso)
                    logger.error(e)
            
            
                logger.debug('len tratti eko {}'.format(len(tratti_Eko)))
                logger.debug('tratti eko {}'.format(tratti_Eko))
                logger.debug('len tratti sit {}'.format(len(tratti_sit)))
                
                logger.debug('len tratti cons {}'.format(len(cod_tratti)))
                s=0
                e=0
                while e < len(tratti_Eko):
                    #logger.debug(f'Ordine SIT {ordine_sit[s]}, {cod_tratti[e][1]}')
                    #logger.debug(f'ID EKO: {tratti_Eko[e]} - Codice EKO {cod_tratti[e][0]} - Tratti SIT {tratti_sit[s]}')
                    logger.debug(f'indice tratti sit {s} - indice tratti eko {e}')
                    if cod_tratti[e][0]!=tratti_sit[s]:
                        logger.warning('Trovato problema')
                        logger.warning(f'ID_EKO:{tratti_Eko[e]},  Posizione EKO:{cod_tratti[e][1]}, Codice eko {cod_tratti[e][0]} - Tratti SIT {tratti_sit[s]}')
                        file_anomalie.write(f'{c};{tratti_Eko[e]};{cod_tratti[e][1]};{cod_tratti[e][0]}\n')                
                    else:
                        s+=1 
                    e+=1
                
                """
                # spunte blu e marroni che sarebbero tappe non correttamente tolte dal percorso da Ekovision per problemi con le date di inizio
                if c not in lista_percorsi_da_verificare:
                    lista_percorsi_da_verificare.append(c) 
                    
    
            
            elif len(tratti_Eko) == len(tratti_sit):
                # devo verificare che siano le stesse componenti
                for ce in tratti_Eko:
                    if dict_tratti_eko_sit[ce] not in tratti_sit:
                        logger.error('PROBLEMA CONFRONTO COMPONENTI')
                        logger.error('Percorso {} - Data {}'.format(c, data_start))
                        logger.error('Tratto ekovision non trovata: {}'.format(dict_tratti_eko_sit[ce]))
                        lista_tratti_anomali.append(dict_tratti_eko_sit[ce])
                        #exit()
            if len(lista_tratti_anomali)>0:
                dict_tratti_non_trovati[c]=lista_tratti_anomali      
            
                      

            
            
        #exit()
        data_start = data_start + timedelta(days=1)
    logger.debug('array id_scheda contiene {}'.format(len(id_scheda)))
    #export text file con lista percorsi da controllare
    outputfile1='{0}/output/percorsi_spazz_spunte_colorate.txt'.format(path,nome)    
    with open(outputfile1, "w") as f:
        for cod_percorso_errori in  lista_percorsi_da_verificare:
            f.write('{}\n'.format(cod_percorso_errori))
    
    
    
    outputfile2='{0}/output/percorsi_spazz_non_presenti.txt'.format(path,nome)  
    with open(outputfile2, "w") as f:
        for k,v in  dict_percorsi_non_presenti.items():
            f.write('{}: {}\n'.format(k,v))
    
    
    
    outputfile3='{0}/output/percorsi_spazz_doppi.txt'.format(path,nome)  
    with open(outputfile3, "w") as f:
        for k,v in  dict_percorsi_doppi.items():
            f.write('{}: {}\n'.format(k,v))
    
    
    
    outputfile4='{0}/output/percorsi_tratti_non_trovati.txt'.format(path,nome)  
    with open(outputfile4, "w") as f:
        for k,v in  dict_tratti_non_trovati.items():
            f.write('{}: {}\n'.format(k,v))    
    
    
    
    file_anomalie.close()
    
    
    # check se c_handller contiene almeno una riga 
    error_log_mail(errorfile, 'assterritorio@amiu.genova.it', os.path.basename(__file__), logger)
    
    
    logger.info("chiudo le connessioni in maniera definitiva")
    curr.close()
    conn.close()
    














if __name__ == "__main__":
    main()      