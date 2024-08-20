#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2023
# Roberto Marzocchi

'''
Lo script si occupa della pulizia dell'elenco percorsi generato dai JOB spoon realizzati per Ekovision

In particolare fa: 

- controllo ed eliminazione percorsi duplicati (non dovrebbe più servire a valle di una modifica al job)
- versionamento dei percorsi come da istruzioni 


'''

#from msilib import type_short
import os, sys, re  # ,shutil,glob

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


path=os.path.dirname(sys.argv[0]) 
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



    
     

def main():
      


    

    
    
    # Get today's date
    #presentday = datetime.now() # or presentday = datetime.today()
    oggi=datetime.today()
    oggi=oggi.replace(hour=0, minute=0, second=0, microsecond=0)
    oggi=date(oggi.year, oggi.month, oggi.day)
    logger.debug('Oggi {}'.format(oggi))
    num_giorno=datetime.today().weekday()
    logger.debug('Giorno della settimana{}'.format(num_giorno))
    if oggi.month == 12 and oggi.day==1 and oggi.year==2024:
        creazione_versioni=1
        logger.info('Oggi è il {} devo creare una nuova versione fino al 31/12/2099'.format(oggi))
    else:
        creazione_versioni=0
        logger.info('Nessuna versione nuova da creare in automatico')
    #num_giorno=datetime.today().weekday()
    #giorno=datetime.today().strftime('%A')
    giorno_file=datetime.today().strftime('%Y%m%d')
    #oggi1=datetime.today().strftime('%d/%m/%Y')
    
    
    # Mi connetto a SIT (PostgreSQL) per poi recuperare le mail
    nome_db=db
    logger.info('Connessione al db {}'.format(nome_db))
    conn = psycopg2.connect(dbname=nome_db,
                        port=port,
                        user=user,
                        password=pwd,
                        host=host)

    
    
    curr = conn.cursor()
    
    
    query_ripassi_fittizi='''select id_percorso, id_asta, num_seq, ripasso_fittizio
                from elem.aste_percorso ap where id_percorso in (
                select id_percorso from elem.percorsi where id_categoria_uso in (3,6)) 
                order by id_percorso, id_asta, num_seq'''
                
                

    try:
        curr.execute(query_ripassi_fittizi)
        lista_aste_percorso=curr.fetchall()
    except Exception as e:
        logger.error(query_percorsi)
        check_error=1
        logger.error(e)

    id_percorso=0
    id_asta=0
    #num_seq=0
    ripasso_fittizio=0
    for aa in lista_aste_percorso:
        if id_percorso==aa[0] and id_asta==aa[1]:
            ripasso_fittizio=ripasso_fittizio+1
            update_1='''update elem.aste_percorso set ripasso_fittizio = %s 
            where id_percorso=%s
            and id_asta = %s and num_seq=%s'''
            try:
                curr.execute(update_1, (ripasso_fittizio, aa[0], aa[1], aa[2]))
            except Exception as e:
                logger.error(update_1)
                logger.error(e)
            
        else :
            ripasso_fittizio=0
        id_percorso=int(aa[0])
        id_asta=int(aa[1])
        #num_seq=aa[2]
        #logger.debug(aa[3])
        if aa[3] is None:
            ripasso_fittizio=0
        else:
            ripasso_fittizio=int(aa[3]) 

    conn.commit()
    curr.close()
    curr = conn.cursor()                

    # PULIZIA TABELLA anagrafe_percorsi.date_percorsi_sit_uo
    
    #STEP 0
    query_delete='''delete from anagrafe_percorsi.date_percorsi_sit_uo where data_inizio_validita = data_fine_validita'''
    try:
        curr.execute(query_delete)
    except Exception as e:
        logger.error(query_delete)
        check_error=1
        logger.error(e)
    curr.close()
    curr = conn.cursor()
    
    
    
    # STEP 1
    query_date_correggere='''select id_percorso_sit, cod_percorso, versioni_uo,
                data_inizio_validita, data_fine_validita 
                from anagrafe_percorsi.date_percorsi_sit_uo dpsu0 where cod_percorso in (
                    select cod_percorso from anagrafe_percorsi.date_percorsi_sit_uo dpsu 
                    group by cod_percorso, data_fine_validita 
                    having count(distinct data_inizio_validita)> 1
                ) order by cod_percorso, data_inizio_validita, data_fine_validita
    '''
    
    try:
        curr.execute(query_date_correggere)
        lista_percorsi_correggere=curr.fetchall()
    except Exception as e:
        logger.error(query_percorsi)
        check_error=1
        logger.error(e)

    c_id_percorso_sit=[]
    c_cod_percorso=[]
    #c_versioni_uo=[]
    c_data_inizio_validita=[]
    c_data_fine_validita=[]
    
    
    for pp in lista_percorsi_correggere:
        c_id_percorso_sit.append(pp[0])
        c_cod_percorso.append(pp[1])
        #versioni_uo.append(pp[2])
        c_data_inizio_validita.append(pp[3])
        c_data_fine_validita.append(pp[4])
    
    i=0
    while i<(len(c_id_percorso_sit)-1):
        if c_cod_percorso[i]==c_cod_percorso[i+1] and c_data_fine_validita[i]==c_data_fine_validita[i+1]:
            update_query='''UPDATE anagrafe_percorsi.date_percorsi_sit_uo 
            set data_fine_validita=%s where id_percorso_sit=%s
            and cod_percorso=%s and data_inizio_validita=%s'''

            try:
                curr.execute(update_query, (c_data_inizio_validita[i+1], c_id_percorso_sit[i], c_cod_percorso[i], c_data_inizio_validita[i]))
            except Exception as e:
                logger.error(update_query)
                logger.error(e)
        i+=1
    
    
    curr.close()
    curr = conn.cursor()
    
    #STEP 2 correggo un secondo caso.. (percorsi dismessi su SIT e non sulla UO)
    query_date_correggere2='''select dpsu.*,  /*p.data_attivazione ,*/p.versione as versione_sit, date_trunc('day',p.data_dismissione) as  data_dismissione
        from anagrafe_percorsi.date_percorsi_sit_uo dpsu
        join elem.percorsi p on p.id_percorso = dpsu.id_percorso_sit 
        where dpsu.cod_percorso in (
            select  cod_percorso
            from anagrafe_percorsi.date_percorsi_sit_uo dpsu1   
            group by cod_percorso, data_fine_validita, data_inizio_validita  
            having count(id_percorso_sit) > 1)
            --and dpsu.cod_percorso = '0101039401'
        order by 2,1
    '''
    
    try:
        curr.execute(query_date_correggere2)
        lista_percorsi_correggere2=curr.fetchall()
    except Exception as e:
        logger.error(query_date_correggere2)
        logger.error(e)

    c2_id_percorso_sit=[]
    c2_cod_percorso=[]
    #c_versioni_uo=[]
    c2_data_inizio_validita=[]
    c2_data_fine_validita=[]
    #c2_versioni_sit=[]
    c2_data_dismissione_sit=[]
    
    for pp in lista_percorsi_correggere2:
        c2_id_percorso_sit.append(pp[0])
        c2_cod_percorso.append(pp[1])
        #versioni_uo.append(pp[2])
        c2_data_inizio_validita.append(pp[3])
        c2_data_fine_validita.append(pp[4])
        c2_data_dismissione_sit.append(pp[6])
        
    
    i=0
    while i<(len(c2_id_percorso_sit)-1):
        if c2_cod_percorso[i]==c2_cod_percorso[i+1] and  c2_data_inizio_validita[i]==c2_data_inizio_validita[i+1] and c2_data_fine_validita[i]==c2_data_fine_validita[i+1]:
            if c2_data_dismissione_sit[i] is None :
                update_query2='''UPDATE anagrafe_percorsi.date_percorsi_sit_uo 
                set data_fine_validita=to_date('31-12-2099', 'DD-MM-YYYY') where id_percorso_sit=%s
                and cod_percorso=%s and data_inizio_validita=%s'''
                try:
                    curr.execute(update_query2, (c2_id_percorso_sit[i], c2_cod_percorso[i], c2_data_inizio_validita[i]))
                except Exception as e:
                    logger.error(update_query2)
                    logger.error(e)
            else :
                update_query2='''UPDATE anagrafe_percorsi.date_percorsi_sit_uo 
                set data_fine_validita=%s where id_percorso_sit=%s
                and cod_percorso=%s and data_inizio_validita=%s'''
                try:
                    curr.execute(update_query2, (c2_data_dismissione_sit[i], c2_id_percorso_sit[i], c2_cod_percorso[i], c2_data_inizio_validita[i]))
                except Exception as e:
                    logger.error(update_query2)
                    logger.error(e)
            # ora correggo la data di inizio della riga i+1    
            update_query2='''UPDATE anagrafe_percorsi.date_percorsi_sit_uo 
            set data_inizio_validita=%s where id_percorso_sit=%s
            and cod_percorso=%s and data_inizio_validita=%s'''
            try:
                curr.execute(update_query2, (c2_data_dismissione_sit[i], c2_id_percorso_sit[i+1], c2_cod_percorso[i+1], c2_data_inizio_validita[i+1]))
            except Exception as e:
                logger.error(update_query2)
                logger.error(e)    
                    
            
        i+=1
     
    # step 3 
    #STEP 2 correggo un secondo caso.. (percorsi dismessi su SIT e non sulla UO)
    query_date_correggere3='''select dpsu.id_percorso_sit, dpsu.cod_percorso, 
 dpsu.data_inizio_validita, dpsu.data_fine_validita 
 from anagrafe_percorsi.date_percorsi_sit_uo dpsu
 join (
    select cod_percorso, data_inizio_validita 
    from anagrafe_percorsi.date_percorsi_sit_uo dpsu2
    group by cod_percorso, data_inizio_validita -- = '0508051203'
    having count(data_fine_validita)>1
 ) anomal on anomal.cod_percorso=dpsu.cod_percorso and anomal.data_inizio_validita = dpsu.data_inizio_validita
    order by 2,4
    '''
    
    try:
        curr.execute(query_date_correggere3)
        lista_percorsi_correggere3=curr.fetchall()
    except Exception as e:
        logger.error(query_date_correggere3)
        check_error=1
        logger.error(e)


    #c3_id_percorso_sit = []
    c3_cod_percorso = []
    c3_data_inizio_validita =[]
    c3_data_fine_validita=[]
    
    
    for pp in lista_percorsi_correggere3:
        #c3_id_percorso_sit.append(pp[0])
        c3_cod_percorso.append(pp[1])
        #versioni_uo.append(pp[2])
        c3_data_inizio_validita.append(pp[2])
        c3_data_fine_validita.append(pp[3])
    
    i=0
    while i<(len(c3_cod_percorso)-1):
        #logger.debug(i)
        #logger.debug(c3_cod_percorso[i])
        if c3_cod_percorso[i].strip()==c3_cod_percorso[i+1].strip() and  c3_data_inizio_validita[i]==c3_data_inizio_validita[i+1] and c3_data_fine_validita[i]!=c3_data_fine_validita[i+1]:
            logger.debug('OK')
            #logger.debug(i)
            #logger.debug(c3_cod_percorso[i])
            update_query3='''UPDATE anagrafe_percorsi.date_percorsi_sit_uo 
                set data_inizio_validita=%s where 
                cod_percorso=%s and data_fine_validita=%s'''
            try:
                curr.execute(update_query3, (c3_data_fine_validita[i], c3_cod_percorso[i+1], c3_data_fine_validita[i+1]))
            except Exception as e:
                logger.error(update_query3)
                logger.error(e)
        i+=1
    conn.commit()
   
    
    
    
    curr.close()
    #exit()
    curr = conn.cursor()
    
    codici=[]
    versioni=[]


    if creazione_versioni==1:
        logger.warning('Questa parte qua la devo pensare un secondo con Riccardo')
        exit()

    query_percorsi='''select cod_percorso, descrizione, id_tipo, freq_testata, id_turno, versione_testata,
data_inizio_validita, data_fine_validita, data_fine_ekovision 
from anagrafe_percorsi.elenco_percorsi ep 
order by cod_percorso, data_fine_validita '''
            
    try:
        curr.execute(query_percorsi)
        lista_percorsi=curr.fetchall()
    except Exception as e:
        logger.error(query_percorsi)
        check_error=1
        logger.error(e)

    i=0
    for pp in lista_percorsi:
        # devo eliminare quelli con id < max(id)
        codici.append(pp[0])
        if i>0:
            if pp[0] == codici[i-1]:
                vers=versioni[i-1]+1
            else:
                vers=1
        else:
            vers=1
        versioni.append(vers)
        logger.debug(pp[7])
        if pp[7] == date(2099, 12, 31):
            if oggi.month<12:
                data_ekovision= date(oggi.year, 12, 31)
            else: 
                data_ekovision= date(oggi.year+1, 12, 31)
            #logger.debug('è 31-12-2099')
        else:
            data_ekovision=pp[7]
            logger.debug('Percorso {} Non è 31-12-2099'.format(pp[0]))
        #logger.debug(pp[5])
        if pp[5] is None or int(pp[5]) != vers or data_ekovision != pp[8]: 
            query_update='''update anagrafe_percorsi.elenco_percorsi ep 
            set versione_testata= %s, data_fine_ekovision=%s
            where cod_percorso= %s and data_inizio_validita=%s'''
            
            try:
                curr.execute(query_update, (vers, data_ekovision, pp[0], pp[6]))
            except Exception as e:
                logger.error(query_update)
                check_error=1
                logger.error(e)
                error_log_mail(errorfile, 'roberto.marzocchi@amiu.genova.it', os.path.basename(__file__), logger)
                exit()
            # continua... 
        i+=1  
 
    conn.commit()
    
    logger.debug('{} Percorsi e {} versioni'.format(len(codici), len(versioni))) 
    
    curr.close()
    curr = conn.cursor()
    
    
    #exit()
    logger.info('Invio dati a Ekovision')
    
    # preparo gli array 

    id_percorso=[]
    descrizione=[]
    id_turno=[] 
    durata=[] 
    id_tipo=[] 
    id_frequenza=[] 
    id_presa_servizio=[]
    id_sede_operativa=[] 
    id_gruppo_coordinamento=[] 
    tipo_ripartizione=[] 
    codice_cer=[]
    codici_cer_compatibili=[]
    data_inizio_validita=[] 
    data_fine_validita=[] 
    versione=[]
    
    
    # li trasmetto tutti
    """query_select='''SELECT cod_percorso as id_percorso, descrizione, id_turno, durata, 
    id_tipo, freq_testata as id_frequenza, cod_sede as id_presa_servizio,
    id_sede_operativa, id_gruppo_coordinamento, 
    tipo_ripartizione, codice_cer, codici_cer_compatibili,
    data_inizio_validita, data_fine_validita, versione
    FROM anagrafe_percorsi.v_servizi_per_ekovision
    order by versione
    '''
    """
    
    if num_giorno==6:
        # nella notte tra sabato e domenica trasmetto tutte le versioni (anche quelle vecchie) dei codici percorsi attivi o in attivazione o disattivati nell'ultimo mese
        query_select='''SELECT cod_percorso as id_percorso, descrizione, id_turno, durata, 
        id_tipo, freq_testata as id_frequenza, cod_sede as id_presa_servizio,
        id_sede_operativa, id_gruppo_coordinamento, 
        tipo_ripartizione, codice_cer, codici_cer_compatibili,
        data_inizio_validita, data_fine_validita, versione
        FROM anagrafe_percorsi.v_servizi_per_ekovision
        where cod_percorso in (
        select distinct cod_percorso from anagrafe_percorsi.elenco_percorsi ep  
        where data_fine_validita >= now()::date or data_ultima_modifica >= now()::date - interval '1' day
        )  or data_fine_validita >= now()::date - interval '1' month
        order by cod_percorso,versione'''
    else:
        # solo incrementale
        query_select='''SELECT cod_percorso as id_percorso, descrizione, id_turno, durata, 
        id_tipo, freq_testata as id_frequenza, cod_sede as id_presa_servizio,
        id_sede_operativa, id_gruppo_coordinamento, 
        tipo_ripartizione, codice_cer, codici_cer_compatibili,
        data_inizio_validita, data_fine_validita, versione
        FROM anagrafe_percorsi.v_servizi_per_ekovision
        where cod_percorso in (
        select distinct cod_percorso from anagrafe_percorsi.elenco_percorsi ep  
        where data_inizio_validita >= now()::date or data_fine_validita = now()::date
        or data_ultima_modifica >= now()::date - interval '1' day
        )   
        order by cod_percorso,versione'''
      
    try:
        curr.execute(query_select)
        lista_servizi=curr.fetchall()
    except Exception as e:
        logger.error(query_select)
        check_error=1
        logger.error(e)
         
    for s in lista_servizi:
        id_percorso.append(s[0])
        descrizione.append(s[1])
        id_turno.append(s[2]) 
        durata.append(s[3])
        id_tipo.append(s[4]) 
        id_frequenza.append(s[5]) 
        id_presa_servizio.append(s[6])
        id_sede_operativa.append(s[7]) 
        id_gruppo_coordinamento.append(s[8]) 
        tipo_ripartizione.append(s[9]) 
        codice_cer.append(s[10])
        codici_cer_compatibili.append(s[11])
        data_inizio_validita.append(s[12].strftime("%Y%m%d")) 
        data_fine_validita.append(s[13].strftime("%Y%m%d")) 
        versione.append(s[14])
      
    
    
    check_ekovision= 200
    try:    
        nome_csv_ekovision="anagrafe_servizi_ekovision.csv"
        file_servizi_ekovision="{0}/log/{1}".format(path,nome_csv_ekovision)
        fp = open(file_servizi_ekovision, 'w', encoding='utf-8')
        '''fieldnames = ['id_percorso', 'descrizione', 'id_turno', 'durata', 
    'id_tipo', 'id_frequenza', 'id_presa_servizio',
    'id_sede_operativa', 'id_gruppo_coordinamento', 
    'tipo_ripartizione', 'codice_cer', 'codici_cer_compatibili',
    'data_inizio_validita', 'data_fine_validita', 'versione']
        '''
        fieldnames = ['id_percorso', 'descrizione', 'id_turno', 'durata', 
    'id_tipo', 'id_frequenza', 'id_presa_servizio',
    'id_sede_operativa', 'id_gruppo_coordinamento', 
    'tipo_ripartizione', 'codice_cer',
    'data_inizio_validita', 'data_fine_validita', 'versione']
      

        #myFile = csv.writer(fp, delimiter=';', quotechar='"', quoting=csv.QUOTE_NONNUMERIC)
        myFile = csv.writer(fp, delimiter=';')
        myFile.writerow(fieldnames)
        
        k=0 
        while k < len(id_percorso):
            row=[id_percorso[k], descrizione[k], id_turno[k], durata[k], 
    id_tipo[k], id_frequenza[k], id_presa_servizio[k],
    id_sede_operativa[k], id_gruppo_coordinamento[k], 
    tipo_ripartizione[k], codice_cer[k],
    data_inizio_validita[k], data_fine_validita[k], versione[k] ]
            myFile.writerow(row)
            k+=1
        '''
        matrice=[tuple(cod_percorso), tuple(data), tuple(id_turno), tuple(id_componente),tuple(id_tratto),
                        tuple(flag_esecuzione), tuple(causale), tuple(nota_causale), tuple(sorgente_dati), tuple(data_ora), tuple(lat), tuple(long)]
        myFile.writerows(matrice)
        '''
        fp.close()
    except Exception as e:
        logger.error(e)
        check_ekovision=102 # problema file variazioni



    logger.info('Invio file versioni percorsi via SFTP')
    try: 
        cnopts = pysftp.CnOpts()
        cnopts.hostkeys = None
        srv = pysftp.Connection(host=url_ev_sftp, username=user_ev_sftp,
    password=pwd_ev_sftp, port= port_ev_sftp,  cnopts=cnopts,
    log="/tmp/pysftp.log")

        with srv.cd('serv_pred/in/'): #chdir to public
            srv.put(file_servizi_ekovision) #upload file to nodejs/

        # Closes the connection
        srv.close()
    except Exception as e:
        logger.error(e)
        check_ekovision=103 # problema invio SFTP  
    
      
    #logger.debug(versioni)
    # check se c_handller contiene almeno una riga 
    error_log_mail(errorfile, 'roberto.marzocchi@amiu.genova.it', os.path.basename(__file__), logger)
    logger.info("chiudo le connessioni in maniera definitiva")
    curr.close()
    conn.close()




if __name__ == "__main__":
    main()      