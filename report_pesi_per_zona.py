#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2021
# Riccardo Piana, Roberto Marzocchi

'''
Lo script parte dal blocchetto creato da R. Piana che si trova nella directory 
ContrattoDiServizio/PesiPercorsi/ReportPesiPercorsi

Per ogni UT raggruppa i percorsi per tipo turno e calcola alcune statistiche di base

Invia il report settimanalmente ai capi zona ...

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


#import requests

import logging

path=os.path.dirname(sys.argv[0]) 
#tmpfolder=tempfile.gettempdir() # get the current temporary directory
logfile='{}/log/check_report_pesi_zona.log'.format(path)
errorfile='{}/log/error_check_report_pesi_zona.log'.format(path)
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


# function to return a named tuple
def makeNamedTupleFactory(cursor):
    columnNames = [d[0].lower() for d in cursor.description]
    import collections
    Row = collections.namedtuple('Row', columnNames)
    return Row


# funzionde per restituire un dizionario
def makeDictFactory(cursor):
    columnNames = [d[0] for d in cursor.description]
    def createRow(*args):
        return dict(zip(columnNames, args))
    return createRow





def main():
    # Mi connetto al DB oracle
    cx_Oracle.init_oracle_client(percorso_oracle) # necessario configurare il client oracle correttamente
    #cx_Oracle.init_oracle_client() # necessario configurare il client oracle correttamente
    parametri_con='{}/{}@//{}:{}/{}'.format(user_uo,pwd_uo, host_uo,port_uo,service_uo)
    logger.debug(parametri_con)
    con = cx_Oracle.connect(parametri_con)
    logger.info("Collegato a DB Oracle. Versione ORACLE: {}".format(con.version))
    cur = con.cursor()


    # Mi connetto a SIT (PostgreSQL) per poi recuperare le mail
    nome_db=db
    logger.info('Connessione al db {}'.format(nome_db))
    conn = psycopg2.connect(dbname=nome_db,
                        port=port,
                        user=user,
                        password=pwd,
                        host=host)

    curr = conn.cursor()


    num_giorno=date.today().weekday()
    logger.debug('num_giorno={}'.format(num_giorno))
    gg=7+num_giorno+1

    logger.info('gg = {}'.format(gg))
    day= date.today() - timedelta(days=(gg-1))
    logger.debug("day={}".format(day))
    
    
    giorno=day.strftime('%A')
    giorno2=day.strftime('%d/%m/%Y')
    giorno_file=day.strftime('%Y%m%d')


    logger.debug(giorno2)
    logger.debug(giorno_file)
    #exit()
    


    # query turni 
    query_fasce_turni= '''SELECT CODICE_TURNO, DESCRIZIONE
     FROM ANAGR_FASCIA_TURNO aft  ORDER BY CODICE_TURNO '''


    try:
        cur.execute(query_fasce_turni)
        # cur.rowfactory = makeNamedTupleFactory(cur)
        cur.rowfactory = makeDictFactory(cur)
        fasce_turno=cur.fetchall()
    except Exception as e:
        logger.error(query_UO)
        logger.error(e)

    fasce=[]
    for ft in fasce_turno:
        fasce.append(ft['DESCRIZIONE'])

    logger.debug(fasce)
    
    # QUERY per estrarre i dati (R.Piana)
    # DA SISTEMARE LE DATE
    query0= ''' select zona_titolare AS zona, desc_ut_esecutrice AS UT_ESECUTRICE, desc_ut_titolare AS ut_titolare,
id_percorso AS codice, descrizione_percorso AS percorso, descrizione_servizio AS servizio, cer,   
tipo_rifiuto, turno, descr_orario AS orario,
data_conferimento, ora_conferimento, tipo_mezzo mezzo, sportello, targa, portata, autista, tipo_record AS prov_registrazione,
destinazione, peso, 
case
when portata > 0 
then  concat(to_char(round(peso*100/portata)),' %') 
else NULL
end
PERC_PORTATA
from dettaglio_pesi_percorsi'''

    query_date='''where to_char(data_conferimento, 'yyyymmdd')||ora_conferimento between to_char(sysdate - :gg , 'yyyymmdd')||'04:00:00'
and to_char(sysdate - (:gg-7), 'yyyymmdd')||'03:59:59' '''

    query='{0} {1}'.format(query0, query_date)

    query_zona_0='''select DISTINCT ZONA_TITOLARE from v_dettaglio_pesi_percorsi'''
    query_zona='{0} {1}'.format(query_zona_0, query_date)
    
    
    query_order='''ORDER BY DATA_CONFERIMENTO, ORA_CONFERIMENTO'''

    query_UO= '{} {}'.format(query, query_order)

    '''try:
        cur.execute(query_UO, [gg])
        # cur.rowfactory = makeNamedTupleFactory(cur)
        cur.rowfactory = makeDictFactory(cur)
        pesi_percorsi=cur.fetchall()
    except Exception as e:
        logging.error(query_UO)
        logging.error(e)
    '''

    '''zona=[]
    ut_es=[]
    ut_resp=[]
    codice=[]
    percorso=[]
    servizio=[]
    CER=[]
    tipo_rifiuto=[]
    turno=[]
    orario=[]
    data_conferimento=[]
    ora_conferimento=[]
    mezzo=[]
    targa=[]
    portata=[] 
    autista=[]
    prov_registrazione=[]
    destinazione=[]
    peso =[]
    perc_portata=[]'''


    try:
        cur.execute(query_zona, (gg,gg))
        zone=cur.fetchall()
    except Exception as e:
        logger.error(query_zona)
        logger.error(e)
    logger.debug(gg)
    logger.debug(zone[0][0])
    #zone=['RIMESSE', 'EXTRA GENOVA', 'ZONA LEVANTE', 'ZONA PONENTE', 'ZONA CENTRO']
    cur.close()
    cur = con.cursor()
    #exit()
    
    
    q_m='''select za.cod_zona, concat(za.mail, ',', string_agg(distinct u.mail, ','))  from topo.ut u
	join topo.zone_amiu za on za.id_zona = u.id_zona
	where cod_zona ilike %s
	group by za.cod_zona, za.mail'''

        
           



    '''mail_cc=['roberto.marzocchi@amiu.genova.it, Riccardo.Piana@amiu.genova.it',
    'roberto.marzocchi@amiu.genova.it, Riccardo.Piana@amiu.genova.it',
    'roberto.marzocchi@amiu.genova.it, Riccardo.Piana@amiu.genova.it',
    'roberto.marzocchi@amiu.genova.it, Riccardo.Piana@amiu.genova.it',
    'roberto.marzocchi@amiu.genova.it, Riccardo.Piana@amiu.genova.it' ]
    '''


    # PER test
    
    mail_cc='assterritorio@amiu.genova.it, Riccardo.Piana@amiu.genova.it, Fabio.Fruscione@amiu.genova.it'
    
    #mail_cc='roberto.marzocchi@amiu.genova.it'


    i=0
    while i < len(zone): 
        
        # cerco la mail a cui inviare i risultati
        try:
            curr.execute(q_m, (zone[i][0],))
            mail=curr.fetchall()
        except Exception as e:
            logger.error(q_m)
            logger.error(e)
        logger.debug(len(mail))
        for mm in mail:
            mail_invio=mm[1]
        
        logger.info('Creo il file per la {}. Creo nuovo file'.format(zone[i][0]))
        nome_file="{0}_{1}.xlsx".format(giorno_file, zone[i][0].replace(' ', '_'))
        file_zone="{0}/zone/{1}".format(path,nome_file)
        


        workbook = xlsxwriter.Workbook(file_zone)
       

        date_format = workbook.add_format({'font_size': 9, 'border':   1,
        'num_format': 'dd/mm/yyyy', 'valign': 'vcenter', 'center_across': True})
        date_format_red = workbook.add_format({'font_size': 9, 'border':   1, 'color': '#ff0000',
        'num_format': 'dd/mm/yyyy', 'valign': 'vcenter', 'center_across': True})
        

        title = workbook.add_format({'bold': True,  'font_size': 9, 'border':   1, 'bg_color': '#F9FF33', 'valign': 'vcenter', 'center_across': True,'text_wrap': True})
        #text_common 
        tc =  workbook.add_format({'border':   1, 'font_size': 9, 'valign': 'vcenter', 'center_across': True, 'text_wrap': True})
        tc_red =  workbook.add_format({'border':   1, 'font_size': 9, 'color': '#ff0000', 'valign': 'vcenter', 'center_across': True, 'text_wrap': True})
        merge_format = workbook.add_format({
                    'bold':     True,
                    'border':   1,
                    'font_size': 9,
                    'align':    'center',
                    'valign':   'vcenter',
                    'color': '#ff0000',
                    'text_wrap': True
                })

        # pezzo di query sulla zona
        query_zona= '''and zona_titolare=:zone'''

        # ciclo sulle UT
        query_ut0='''select distinct desc_ut_titolare from v_dettaglio_pesi_percorsi'''
        query_ut='{0} {1} {2}'.format(query_ut0, query_date, query_zona)


        try:
            cur.execute(query_ut, (gg, gg, zone[i][0]))
            # cur.rowfactory = makeNamedTupleFactory(cur)
            cur.rowfactory = makeDictFactory(cur)
            ut_titolari=cur.fetchall()
        except Exception as e:
            logger.error(query_ut)
            logger.error('gg={} and zona ={}'.format(gg, zone[i][0]))
            logger.error(e)

        for ut in ut_titolari:
            logger.debug(ut['DESC_UT_TITOLARE'])

            # creo il foglio per ogni UT
            if len(ut['DESC_UT_TITOLARE'])>31:
                w = workbook.add_worksheet(ut['DESC_UT_TITOLARE'].replace('.','').replace(' - ', '_')[1:31])
            else:    
                w = workbook.add_worksheet(ut['DESC_UT_TITOLARE'])

            #imposto la larghezza delle 19 colonne
            w.set_column(0, 19, 12)


            # ciclo sui turni 
            t=0
            #parto a scrivere dalla riga 1
            rr=1
            # Array dove scrivere i pesi per fasce turno
            pesi_altro=[]
            pesi_rsu=[]
            cont_turni=[]
            fasce_presenti=[]
            while t<len(fasce):
                

                query_turno = '''and turno = :fascia '''

                query_UO= '{0} {1} {2} and DESC_UT_TITOLARE=:ut_titolare {3}'.format(query, query_zona, query_turno, query_order)

                #logger.debug(query_UO)
                #exit()

                try:
                    cur.execute(query_UO, [gg, gg, zone[i][0], fasce[t], ut['DESC_UT_TITOLARE']])
                    # cur.rowfactory = makeNamedTupleFactory(cur)
                    cur.rowfactory = makeDictFactory(cur)
                    pesi_percorsi=cur.fetchall()
                except Exception as e:
                    logger.error(query_UO)
                    logger.error(gg, zone[i][0], fasce[t], ut['DESC_UT_TITOLARE'])
                    logger.error(e)
                #for pp in pesi_percorsi:
                    #print(pp['ZONA'])
                    #exit()
                #print(pesi_percorsi)
                
                
                
                if len(pesi_percorsi)> 0:
                    #Questo serviva per mettere i totali sotto i singoli turni 
                    #if t>0:
                    #    rr=rr+2
                    cont=1
                    peso_rsu=0
                    peso_altro=0
                    #logger.debug('TURNO={}, rr= {} cont={}'.format(fasce[t], rr, cont))
                    for pp in pesi_percorsi:
                        cc=0
                        for key, value in pp.items():
                            #print(key)
                            #print(value)
                            
                            w.write(0, cc, key.replace('_', ' '), title)
                            
                            
                            #if pp['PERC_PORTATA'] is not None:
                            if pp['PERC_PORTATA'] is not None:
                                if  int(pp['PERC_PORTATA'].replace(' %',''))>100:
                                    stile=tc_red
                                    stile_date=date_format_red
                                else:
                                    stile=tc
                                    stile_date=date_format
                            if type(value) is str:
                                w.write(rr, cc, value, stile)
                            elif type(value) is datetime :
                                w.write(rr, cc, value, stile_date)
                                #logger.debug(type(value))
                            elif type(value) is int :
                                w.write(rr, cc, value, stile)
                            cc+=1

                        # alla chiusura scrivo le sommatorie
                        if pp['CER']=='200301' :
                            peso_rsu += pp['PESO']
                        else:
                            peso_altro += pp['PESO']


                        
                        
                        if cont==len(pesi_percorsi) and cont > 0:
                            pesi_altro.append(peso_altro)
                            pesi_rsu.append(peso_rsu)
                            cont_turni.append(cont)
                            fasce_presenti.append(fasce[t])
                            '''
                            w.merge_range(rr+1, 2, rr+2, 2, 'Totale turno {}'.format(fasce[t]), merge_format)
                            w.write(rr+1, 4, '{} servizi'.format(cont), merge_format)
                            w.write(rr+1, 5, 'Totale RD', merge_format)
                            w.write(rr+2, 5, 'Totale RSU', merge_format)
                            w.write(rr+1, 6, peso_altro, merge_format)
                            w.write(rr+2, 6, peso_rsu, merge_format)
                            w.write(rr+1, 7, rr+2, 7, ' % RD TURNO {}'.format(fasce[t]), merge_format)
                            w.write(rr+2, 8, '{} %'.format(round(peso_altro*100/(peso_altro+peso_rsu))), merge_format)
                            '''
                        cont+=1
                        rr += 1

                    '''for key, value in pesi_percorsi:
                        w.write(0, col_num, key)
                        w.write_column(1, col_num, value)
                        col_num += 1
                    '''
                # vado avanti al turno successivo
                t+=1
            rr_filtro=rr-1
            w.autofilter(0, 0, rr-1, cc-1)
            k=0
            logger.debug(fasce_presenti)
            logger.debug(cont_turni)
            #exit()
            while k<len(fasce_presenti):
                if k==0:
                    rr+=1
                w.merge_range(rr, 4, rr, 8, 'Totale turno {}'.format(fasce_presenti[k]), merge_format)
                w.write(rr+1, 4, '{} servizi'.format(cont_turni[k]), merge_format)
                w.write(rr+1, 5, 'Totale RD', merge_format)
                w.write(rr+2, 5, 'Totale RSU', merge_format)
                w.write(rr+1, 6, pesi_altro[k], merge_format)
                w.write(rr+2, 6, pesi_rsu[k], merge_format)
                w.merge_range(rr+1, 7, rr+2, 7, ' % RD TURNO {}'.format(fasce_presenti[k]), merge_format)
                w.merge_range(rr+1, 8, rr+2, 8,'{} %'.format(round(pesi_altro[k]*100/(pesi_altro[k]+pesi_rsu[k]))), merge_format)
                k+=1
                rr+=3
                    

        workbook.close()
        subject = "Report pesi {} ".format(zone[i][0])

        body='''Report pesi settimana passata:<br>
    <ul>
    <li> Zona:    {0}</li>
    <li> Settimana con inizio il {1}</li>
    </ul>
    La presente mail è inviata in automatico dal file report_pesi_per_zona.py del server amiugis realizzato dal <i>SIGT - Gestione e sviluppo applicativi</i>. 
    <br> Per segnalare problemi al report contattare Riccardo Piana Riccardo.Piana@amiu.genova.it e Roberto Marzocchi Roberto.Marzocchi@amiu.genova.it
    <br> Per segnalare problemi sull'assegnazione percorsi contattare Marzocchi/Magioncalda assterritorio@amiu.genova.it
    <br> Per segnalare anomalie sulle portate dei mezzi contattare l'ufficio automezzi Trapasso/Galleno
    <br>
    <img src="cid:image1" alt="Logo" width=197>
    <br>
    '''.format(zone[i][0], giorno2)

        #logger.debug(body)  


        # messaggio='Test invio messaggio'


        #body = "Report giornaliero delle variazioni.\n Giorno {}\n\n".format(giorno_file)
        #sender_email = user_mail
        #receiver_email='assterritorio@amiu.genova.it'
        #debug_email='roberto.marzocchi@amiu.genova.it'

        receiver_email=mail_invio

        # Create a multipart message and set headers
        message = MIMEMultipart()
        message["From"] = 'no_reply@amiu.genova.it'
        # PER TEST (tolgo l'invio ai capi zona e lo metto solo agli indirizzi in CC che siamo noi)
        message["To"] = receiver_email
        #message["To"] = mail_cc
        ####################################################
        message["Subject"] = subject
        message["Bcc"] = mail_cc  # Recommended for mass emails
        message.preamble = "Report pesi"


            
                            
        # Add body to email
        message.attach(MIMEText(body, "html"))

        # aggiunto allegato (usando la funzione importata)
        allegato(message, file_zone, nome_file)
        # Add body to email
        #message.attach(MIMEText(body, "plain"))
        
        #aggiungo logo 
        logoname='{}/img/logo_amiu.jpg'.format(path)
        immagine(message,logoname)
        
        #text = message.as_string()

        logger.info("Richiamo la funzione per inviare mail")
        invio=invio_messaggio(message)
        logger.info(invio)
        if invio==200:
            logger.info('Messaggio inviato')

        else:
            logger.error('Problema invio mail. Error:{}'.format(invio))





    

        # passo alla zona successiva
        i+=1




    # check se c_handller contiene almeno una riga 
    error_log_mail(errorfile, 'roberto.marzocchi@amiu.genova.it, Riccardo.Piana@amiu.genova.it', os.path.basename(__file__), logger)
    logger.info("chiudo le connessioni in maniera definitiva")
    cur.close()
    con.close()

if __name__ == "__main__":
    main()  
