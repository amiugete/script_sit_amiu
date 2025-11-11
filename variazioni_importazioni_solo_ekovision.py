#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2021
# Roberto Marzocchi

'''
Script derivante da variazioni_importazioni.py a cui è stata rimossa la parte relativa alla UO

Lo script verifica le variazioni  per una lista di percorsi dati (array cod_percorso_reimp) o per tutti i percorsi modificati il giorno precedente (vedi ## OGGETTO INVIO ) 
e salva i csv su spazio SFTP di ekovision
'''

import os, sys, re  # ,shutil,glob
import inspect, os.path

import xlsxwriter


import csv

#import getopt  # per gestire gli input

#import pymssql

import psycopg2

import cx_Oracle

import datetime
import holidays
from workalendar.europe import Italy


from credenziali import db, port, user, pwd, host, user_mail, pwd_mail, port_mail, smtp_mail


#import requests

import logging
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


from crea_dizionario_da_query import *



# per mandare file a EKOVISION
import pysftp

#LOG

filename = inspect.getframeinfo(inspect.currentframe()).filename
path     = os.path.dirname(os.path.abspath(filename))

'''#path=os.path.dirname(sys.argv[0]) 
#tmpfolder=tempfile.gettempdir() # get the current temporary directory
logfile='{}/log/variazioni_importazioni.log'.format(path)
#if os.path.exists(logfile):
#    os.remove(logfile)

logging.basicConfig(format='%(asctime)s\t%(levelname)s\t%(message)s',
    filemode='a', # overwrite or append
    filename=logfile,
    level=logging.DEBUG)
'''


path=os.path.dirname(sys.argv[0]) 
#tmpfolder=tempfile.gettempdir() # get the current temporary directory
logfile='{}/log/variazioni_importazioni_test_ekovision.log'.format(path)
errorfile='{}/log/error_variazioni_importazioni_test_ekovision.log'.format(path)
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





def cfr_tappe(tappe_sit, tappe_uo, logger):
    ''' Effettua il confronto fra le tappe di SIT e quelle di UO'''
    #logger.info('Richiamo la funzione cfr_tappe')
    check=0
    if len(tappe_sit) == len(tappe_uo) :
        k=0
        while k < len(tappe_sit):
            #logger.debug(tappe_sit[k][0])
            #logger.debug(tappe_uo[k][0])
            
            # nume_seq 0
            if tappe_sit[k][0]!=tappe_uo[k][0]:
                check=1
            # id_via 1
            if tappe_sit[k][1]!=tappe_uo[k][1]:
                check=1    
            # riferimento 3
            if (tappe_uo[k][3] is None and tappe_sit[k][3] is None) or ( (not tappe_uo[k][3] or re.search("^\s*$", tappe_uo[k][3])) and (not tappe_sit[k][3] or re.search("^\s*$", tappe_sit[k][3])) ):
                check1=0
            else:
                if tappe_sit[k][3]!=tappe_uo[k][3]:
                    check=1
                    logger.warning('rif SIT = .{}., rif UO = {}'.format(tappe_sit[k][3], tappe_uo[k][3]))
                    
                
            # frequenza 4
            if tappe_sit[k][4]!=tappe_uo[k][4]:
                check=1   
            # tipo_el 5
            if tappe_sit[k][5]!=tappe_uo[k][5]:
                check=1   
            #id_el 6
            if tappe_sit[k][6]!=tappe_uo[k][6]:
                check=1   
            # nota via  7
            if (tappe_uo[k][7] is None and tappe_sit[k][7] is None) or ( (not tappe_uo[k][7] or re.search("^\s*$", tappe_uo[k][7])) and (not tappe_sit[k][7] or re.search("^\s*$", tappe_sit[k][7])) ):
                check1=0
            else:
                if tappe_sit[k][7]!=tappe_uo[k][7]:
                    check=1
                    logger.warning('SIT =  {}, UO = {}'.format(tappe_sit[k][7], tappe_uo[k][7]))
            
            k+=1
    else:
        check=1
    return check



def main():
    # carico i mezzi sul DB PostgreSQL
    logger.info('Connessione al db')
    conn = psycopg2.connect(dbname=db,
                        port=port,
                        user=user,
                        password=pwd,
                        host=host)

    curr = conn.cursor()
    #conn.autocommit = True


    # Mi connetto al DB oracle UO
    cx_Oracle.init_oracle_client(percorso_oracle) # necessario configurare il client oracle correttamente
    #cx_Oracle.init_oracle_client() # necessario configurare il client oracle correttamente
    parametri_con='{}/{}@//{}:{}/{}'.format(user_uo,pwd_uo, host_uo,port_uo,service_uo)
    logger.debug(parametri_con)
    con = cx_Oracle.connect(parametri_con)
    logger.info("Versione ORACLE: {}".format(con.version))
    
    
    

    oggi=datetime.datetime.today()
    oggi=oggi.replace(hour=0, minute=0, second=0, microsecond=0)
    oggi=datetime.date(oggi.year, oggi.month, oggi.day)
    logging.debug('Oggi {}'.format(oggi))
    
    
    num_giorno=datetime.datetime.today().weekday()
    giorno=datetime.datetime.today().strftime('%A')
    giorno_file=datetime.datetime.today().strftime('%Y%m%d')
    oggi1=datetime.datetime.today().strftime('%d/%m/%Y')
    logging.debug('Il giorno della settimana è {} o meglio {}'.format(num_giorno, giorno))
    
    
    
    holiday_list = []
    holiday_list_pulita=[]
    for holiday in holidays.Italy(years=[(oggi.year -1), (oggi.year)]).items():
        #print(holiday[0])
        #print(holiday[1])
        holiday_list.append(holiday)
        holiday_list_pulita.append(holiday[0])
    
    
    # AGGIUNGO LA FESTA PATRONALE
    logging.debug('Anno corrente = {}'.format(oggi.year))
    fp = datetime.datetime(oggi.year, 6, 24)
    festa_patronale=datetime.date(fp.year, fp.month, fp.day)
    holiday_list_pulita.append(festa_patronale)
    
    if num_giorno==0:
        num=3
        # controllo se venerdì era festivo
        ven = oggi - datetime.timedelta(days = num)
        ven=datetime.date(ven.year, ven.month, ven.day)
        if ven in holiday_list_pulita:
            num=4
            gio = oggi - datetime.timedelta(days = num)
            gio=datetime.date(gio.year, gio.month, gio.day)
            if gio in holiday_list_pulita:
                num=5
    elif num_giorno in (5,6):
        num=0
        logging.info('Oggi è {0}, lo script non gira'.format(giorno))
        exit()
    else:
        num=1
        # se oggi è festa
        if oggi in holiday_list_pulita:
            num=0
            logging.info('Oggi è giorno festivo, lo script non gira'.format(giorno))
            exit()
        ieri=oggi - datetime.timedelta(days = num)
        ieri=datetime.date(ieri.year, ieri.month, ieri.day)
        #logging.debug('Ieri = {}'.format(ieri))
        #logging.debug(holiday_list_pulita)
        if ieri in holiday_list_pulita:
            # se ieri era lunedì (es. Pasquetta)
            logging.debug('Ieri {}'.format(ieri.strftime('%A')))
            if ieri.weekday()==0:
                num=4 # da ven in poi
            # se ieri era martedì
            elif ieri.weekday()==1:
                num=2
                # verifico altro ieri 
                altroieri=oggi - datetime.timedelta(days = num)
                altroieri=datetime.date(altroieri.year, altroieri.month, altroieri.day)
                # se altro ieri era festivo e lunedì (caso di Natale lunedì e S. Stefano Martedì)
                if altroieri in holiday_list_pulita:
                    num=5
            # altrimenti
            else: 
                num=2
                # verifico altro ieri 
                altroieri=oggi - datetime.timedelta(days = num)
                altroieri=datetime.date(altroieri.year, altroieri.month, altroieri.day)
                # se altro ieri era festivo e non lunedì (caso di Natale martedì/mercoledì o di due feste vicine)
                if altroieri in holiday_list_pulita:
                    num=3
                    
    
    logging.debug('num = {}'.format(num))
                    
                    
                    
    
    
    '''******************************************************************************************************
    NON SONO COMPRESI I PERCORSI STAGIONALI per cui vanno re-importate le variazioni in fase di attivazione 
    ********************************************************************************************************'''
    query='''select distinct p.cod_percorso , p.descrizione, s.descrizione as servizio, u.descrizione  as ut,
        p.id_percorso
        from util.sys_history h
        inner join elem.percorsi p 
        on h.id_percorso = p.id_percorso 
        inner join elem.percorsi_ut pu 
        on pu.cod_percorso =p.cod_percorso 
        inner join elem.servizi s 
        on s.id_servizio =p.id_servizio
        inner join topo.ut u 
        on u.id_ut = pu.id_ut 
        where h.datetime > (current_date - INTEGER '{0}') 
        and h.datetime <= current_date::date 
        and (
        (h."type" IN ('PERCORSO') 
        and h.action IN ('UPDATE_ELEM', 'UPDATE')
        ) or 
        (h."type" IN ('ASTA PERCORSO') 
        and h.action IN ('INSERT', 'UPDATE', 'DELETE')
        )
        )
        and pu.responsabile = 'S'
        and p.id_categoria_uso in (3)
        and (p.data_dismissione is null or p.data_dismissione > current_date )
        and p.data_attivazione <= current_date::date
        UNION 
        /*PERCORSI CHE SI ATTIVANO QUEL GIORNO (stagionali e non solo)*/
        select p2.cod_percorso , p2.descrizione, s2.descrizione as servizio, u2.descrizione  as ut, 
        p2.id_percorso
        from elem.percorsi p2 
        join elem.servizi s2 on s2.id_servizio = p2.id_servizio 
        inner join elem.percorsi_ut pu2 
        on pu2.cod_percorso =p2.cod_percorso
        inner join topo.ut u2 
        on u2.id_ut = pu2.id_ut 
        where pu2.responsabile = 'S'
        and p2.id_categoria_uso in (3)
        and p2.data_attivazione > (current_date - INTEGER '{0}')
        and p2.data_attivazione <= current_date::date
        UNION
        select distinct p3.cod_percorso , p3.descrizione, s3.descrizione as servizio, u3.descrizione  as ut,
        p3.id_percorso
        from elem.elementi e
        join (
        select datetime, description, id_piazzola, split_part(replace(description, 'Elementi tipo ', ''), ' ',1) as tipo_elemento 
        from util.sys_history sh 
        where type='PIAZZOLA_ELEM' and action = 'UPDATE' and description ilike 'Elementi tipo%' 
        and sh.datetime > (current_date - INTEGER '{0}')
        and sh.datetime <= current_date::date
        and id_percorso is null 
        ) b on b.id_piazzola=e.id_piazzola and b.tipo_elemento::int = e.tipo_elemento and date_trunc('second', e.data_inserimento) != date_trunc('second', b.datetime)  
        join elem.elementi_aste_percorso eap on eap.id_elemento = e.id_elemento 
        join elem.aste_percorso ap on eap.id_asta_percorso = ap.id_asta_percorso 
        join elem.percorsi p3 on p3.id_percorso = ap.id_percorso 
        join elem.servizi s3 on s3.id_servizio = p3.id_servizio 
        inner join elem.percorsi_ut pu3 
        on pu3.cod_percorso =p3.cod_percorso
        inner join topo.ut u3 
        on u3.id_ut = pu3.id_ut 
        where pu3.responsabile = 'S'
        and p3.id_categoria_uso in (3)
        and (p3.data_dismissione is null or p3.data_dismissione > current_date )
        and p3.data_attivazione <= current_date::date
        order by ut, servizio
        '''.format(num)
    


    try:
        curr.execute(query)
        lista_variazioni=curr.fetchall()
    except Exception as e:
        logger.error(e)


    #inizializzo gli array
    cod_percorso=[]
    descrizione=[]
    servizio=[]
    ut=[]
    stato_importazione=[]

           
    #exit()
       
    #print(cod_percorso)
    cod_percorso_reimp=['0101000301',
'0101007101',
'0101032103',
'0101034101',
'0101040703',
'0101042102',
'0101359802',
'0101361301',
'0101361401',
'0101362801',
'0101363103',
'0101363501',
'0101363801',
'0101366801',
'0101366901',
'0101370001',
'0101378603',
'0101379102',
'0101381203',
'0101383601',
'0101387809',
'0101387909',
'0101388009',
'0101388409',
'0101392301',
'0102008201',
'0111003703',
'0111003903',
'0111007703',
'0111008902',
'0201255901',
'0201257601',
'0203008701',
'0213251002',
'0500003701',
'0500100302',
'0500103101',
'0500106301',
'0500121703',
'0500121803',
'0500122603',
'0500124302',
'0500125103',
'0500125302',
'0500126202',
'0500131903',
'0500132502',
'0500132702',
'0501008301',
'0501009101',
'0501010409',
'0501011309',
'0501014209',
'0501016801',
'0501018801',
'0501019401',
'0501020101',
'0501021501',
'0502002603',
'0502002703',
'0502041001',
'0503001601',
'0506006501',
'0507001801',
'0507002702',
'0507106202',
'0507107402',
'0507109402',
'0507113901',
'0507120201',
'0507128501',
'0507128903',
'0507129603',
'0507130103',
'0507130201',
'0507134301',
'0507134401',
'0507137901',
'0508035701',
'0508054201',
'0508060701',
'0508069809',
'0508071901',
'0508072301',
'0508072401',
'0508072801',
'0508073102',
'0508073401',
'0508076801',
'0508077903'
                        ]
    
    logger.info('Itinerari da esportare :{}'.format(cod_percorso_reimp))
    
    
    curr.close()
    logger.info('Ora invio le variazioni ad EKOVISION')
    check_ekovision=0
    logger.debug(cod_percorso)
    cod_percorso_ok=tuple(cod_percorso)
    logger.debug(cod_percorso_ok)
    curr = conn.cursor()  
    query_variazioni_ekovision='''select 
codice_modello_servizio,
coalesce((select distinct ordine from anagrafe_percorsi.v_percorsi_elementi_tratti 
where codice_modello_servizio = tab.codice_modello_servizio 
and codice = tab.codice
and ripasso = tab.ripasso and data_fine is null limit 1 ),1)
as ordine,
objecy_type, 
  codice, quantita, lato_servizio, percent_trattamento,
coalesce((select distinct frequenza from anagrafe_percorsi.v_percorsi_elementi_tratti 
where codice_modello_servizio = tab.codice_modello_servizio 
and codice = tab.codice
and ripasso = tab.ripasso and data_fine is null limit 1),0)
as 
  frequenza, 
  numero_passaggi, nota,
  codice_qualita, codice_tipo_servizio,
min(data_inizio) as data_inizio, 
case 
	when max(data_fine) = '20991231' then null 
	else max(data_fine)
end data_fine, 
/*ripasso*/
case 
	when max(data_fine) = '20991231' then ripasso 
	else 0
end ripasso
from (
	  SELECT codice_modello_servizio, ordine, objecy_type, 
  codice, quantita, lato_servizio, percent_trattamento,frequenza,
  ripasso, numero_passaggi, replace(replace(coalesce(nota,''),'DA PIAZZOLA',''),';', ' - ') as nota,
  codice_qualita, codice_tipo_servizio, data_inizio, coalesce(data_fine, '20991231') as data_fine
	 FROM anagrafe_percorsi.v_percorsi_elementi_tratti where data_inizio <= coalesce(data_fine, '20991231')
	 union 
	   SELECT codice_modello_servizio, ordine, objecy_type, 
  codice, quantita, lato_servizio, percent_trattamento,frequenza,
  ripasso, numero_passaggi, replace(replace(coalesce(nota,''),'DA PIAZZOLA',''),';', ' - ') as nota,
  codice_qualita, codice_tipo_servizio, data_inizio, coalesce(data_fine, '20991231') as data_fine
	 FROM anagrafe_percorsi.v_percorsi_elementi_tratti_ovs where data_inizio <= coalesce(data_fine, '20991231')
    union 
  SELECT codice_modello_servizio, ordine, objecy_type, 
    codice, quantita, lato_servizio, percent_trattamento,frequenza,
    ripasso, numero_passaggi, replace(replace(coalesce(nota,''),'DA PIAZZOLA',''),';', ' - ') as nota,
    codice_qualita, codice_tipo_servizio, data_inizio, coalesce(data_fine, '20991231') as data_fine
        FROM anagrafe_percorsi.mv_percorsi_elementi_tratti_dismessi where data_inizio <= coalesce(data_fine, '20991231')
 ) tab 
 where codice_modello_servizio = ANY (%s) 
 group by codice_modello_servizio,  objecy_type, 
  codice, quantita, lato_servizio, percent_trattamento,
  ripasso, numero_passaggi, nota,
  codice_qualita, codice_tipo_servizio
  order by codice_modello_servizio, data_fine asc, ordine,  ripasso'''
    
    #test=curr.mogrify(query_variazioni_ekovision,(cod_percorso_ok,))
    #print(test)
    #exit()
    try:
        ## OGGETTO INVIO: 
        ##mando tutti i percorsi modificati il giorno precedente
        #curr.execute(query_variazioni_ekovision,(cod_percorso,))

        ## OGGETTO INVIO:
        ## mando solo i percorsi definiti nella lista cod_percorso_reimp
        curr.execute(query_variazioni_ekovision,(cod_percorso_reimp,))
        dettaglio_percorsi_ekovision=curr.fetchall()
    except Exception as e:
        logger.error(e)
        check_ekovision=101 # problema query
    
    try:    
        nome_csv_ekovision="variazioni_itinerari_{0}.csv".format(giorno_file)
        file_variazioni_ekovision="{0}/variazioni/{1}".format(path,nome_csv_ekovision)
        fp = open(file_variazioni_ekovision, 'w', encoding='utf-8')
        #myFile = csv.writer(fp, delimiter=';', quotechar='"', quoting=csv.QUOTE_NONNUMERIC)
        myFile = csv.writer(fp, delimiter=';')
        fieldnames = ['codice_modello_servizio', 'ordine', 'objecy_type', 
                      'codice','quantita', 'lato_servizio', 'percent_trattamento',
                      'frequenza', 'numero_passaggi', 'nota', 'codice_qualita', 'codice_tipo_servizio',
                      'data_inizio', 'data_fine', 'ripasso']
        myFile.writerow(fieldnames)
        myFile.writerows(dettaglio_percorsi_ekovision)
        fp.close()
    except Exception as e:
        logger.error(e)
        check_ekovision=102 # problema file variazioni
        
        
        
    logger.info('Invio file con le variazioni via SFTP')
    try: 
        cnopts = pysftp.CnOpts()
        cnopts.hostkeys = None
        srv = pysftp.Connection(host=url_ev_sftp, username=user_ev_sftp,
    password=pwd_ev_sftp, port= port_ev_sftp,  cnopts=cnopts,
    log="/tmp/pysftp.log")

        with srv.cd('percorsi/in/'): #chdir to public
            srv.put(file_variazioni_ekovision) #upload file to nodejs/

        # Closes the connection
        srv.close()
    except Exception as e:
        logger.error(e)
        check_ekovision=103 # problema invio SFTP

    exit()

    
       
    if len(cod_percorso)>0:
        logger.info('Oggi ci sono {} variazioni. Creo nuovo file'.format(len(cod_percorso)))
        nome_file="{0}_variazioni.xlsx".format(giorno_file)
        file_variazioni="{0}/variazioni/{1}".format(path,nome_file)
        
        
        workbook = xlsxwriter.Workbook(file_variazioni)
        w = workbook.add_worksheet()

        w.write(0, 0, 'cod_percorso') 
        w.write(0, 1, 'descrizione') 
        w.write(0, 2, 'servizio') 
        w.write(0, 3, 'ut') 
        w.write(0, 4, 'ESITO IMPORTAZIONE') 
        
        '''
        w.write(1, 0, 1234.56)  # Writes a float
        w.write(2, 0, 'Hello')  # Writes a string
        w.write(3, 0, None)     # Writes None
        w.write(4, 0, True)     # Writes a bool
        '''
        
        #f = open(file_variazioni, "w")
        #f.write('cod_percorso;descrizione;servizio;ut_resp\n')
    


    i=0
    while i<len(cod_percorso):
        #f.write('"{}";"{}";"{}";"{}"\n'.format(cod_percorso[i],descrizione[i],servizio[i],ut[i]))
        w.write(i+1,0,'{}'.format(cod_percorso[i]))
        w.write(i+1,1,'{}'.format(descrizione[i]))
        w.write(i+1,2,'{}'.format(servizio[i]))
        w.write(i+1,3,'{}'.format(ut[i]))
        w.write(i+1,4,'{}'.format(stato_importazione[i]))
        i+=1

    if len(cod_percorso)>0:
        #f.close()
        workbook.close()

    #exit() # per ora esco qua e non vado oltre

    
    # Create a secure SSL context
    context = ssl.create_default_context()



   # messaggio='Test invio messaggio'


    subject = "Variazioni odierne - File automatico"
    if num==1:
        gg_text='''dell'ultimo giorno (ieri)'''
    else:
        gg_text='''degli ultimi {} giorni'''.format(num)
    body = """Report giornaliero delle variazioni degli ultimi {0} giorni.<br><br>
    
    <b>IN TEST </b> - I nuovi percorsi sono già stati importati. Verificare la corretta importazione. {3}<br><br><br> 
    
    L'applicativo che gestisce le importazioni su UO in maniera automatica è stato realizzato dal gruppo Gestione Applicativi del SIGT.<br> 
    Segnalare tempestivamente eventuali malfunzionamenti inoltrando la presente mail a {1}<br><br>
    Giorno {2}<br><br>
    AMIU Assistenza Territorio<br>
     <img src="cid:image1" alt="Logo" width=197>
    <br>
    """.format(gg_text, user_mail, oggi1, nota_f_mail)
    ##sender_email = user_mail
    receiver_email='assterritorio@amiu.genova.it'
    debug_email='roberto.marzocchi@amiu.genova.it'

    # Create a multipart message and set headers
    message = MIMEMultipart()
    message["From"] = sender_email
    message["To"] = receiver_email
    message["Subject"] = subject
    #message["Bcc"] = debug_email  # Recommended for mass emails
    message.preamble = "File giornaliero con le variazioni"


        
                        
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
    
    

    
    ##################################################################################################
    ####                                SISTEMO LE SEQUENZE 
    ##################################################################################################
    
    cur0 = con.cursor()
    cur1 = con.cursor()
    cur2 = con.cursor()
    
    
    sel_uo0='''SELECT last_number
    FROM user_sequences
    WHERE sequence_name ='SEQ_ID_MACRO_TAPPA' ''' 
    try:
        cur0.execute(sel_uo0)
        #cur1.rowfactory = makeDictFactory(cur1)
        current_seq=cur0.fetchall()
    except Exception as e:
        logger.error(sel_uo)
        logger.error(e)
        
    cur0.close() 
    
    
    sel_uo='''SELECT max(ID_MACRO_TAPPA) FROM CONS_MACRO_TAPPA cmt ''' 
    try:
        cur1.execute(sel_uo)
        #cur1.rowfactory = makeDictFactory(cur1)
        max_macro=cur1.fetchall()
    except Exception as e:
        logger.error(sel_uo)
        logger.error(e)
    
    max=max_macro[0][0]
    
    cur1.close()      

    if (current_seq[0][0]<max):
        check=0
        logger.debug('La sequenza è da correggere')
    else:
        logger.debug('La sequenza è OK, non devo fare nulla')
        check=2
    
    if check ==0:
        logger.info(' Faccio un ciclo per portare la sequenza fino al valore massimo di {}'.format(max))
    
    
    while check==0:
        sel_uo2='''select SEQ_ID_MACRO_TAPPA.NEXTVAL from dual'''
        try:
            cur2.execute(sel_uo2)
            #cur1.rowfactory = makeDictFactory(cur1)
            seq_macro=cur2.fetchall()
        except Exception as e:
            logger.error(sel_uo2)
            logger.error(e)
        logger.debug('max={} macro={}'.format(max,seq_macro[0][0]))
        if seq_macro[0][0]== max:
            check=1
        #exit()
    

    cur2.close()      
    
    
    
    
    ## MICRO TAPPE
    
    cur0 = con.cursor()
    cur1 = con.cursor()
    cur2 = con.cursor()
    
    
    sel_uo0='''SELECT last_number
    FROM user_sequences
    WHERE sequence_name ='SEQ_ID_MICRO_TAPPA' ''' 
    try:
        cur0.execute(sel_uo0)
        #cur1.rowfactory = makeDictFactory(cur1)
        current_seq=cur0.fetchall()
    except Exception as e:
        logger.error(sel_uo)
        logger.error(e)
        
    cur0.close() 
    
    
    sel_uo='''SELECT max(ID_MICRO_TAPPA) FROM CONS_MICRO_TAPPA cmt ''' 
    try:
        cur1.execute(sel_uo)
        #cur1.rowfactory = makeDictFactory(cur1)
        max_micro=cur1.fetchall()
    except Exception as e:
        logger.error(sel_uo)
        logger.error(e)
    
    max=max_micro[0][0]
    
    cur1.close()      

    if (current_seq[0][0]<max):
        check=0
        logger.debug('La sequenza è da correggere')
    else:
        logger.debug('La sequenza è OK, non devo fare nulla')
        check=2
    
    if check ==0:
        logger.info(' Faccio un ciclo per portare la sequenza fino al valore massimo di {}'.format(max))
    
    
    while check==0:
        sel_uo2='''select SEQ_ID_MICRO_TAPPA.NEXTVAL from dual'''
        try:
            cur2.execute(sel_uo2)
            #cur1.rowfactory = makeDictFactory(cur1)
            seq_micro=cur2.fetchall()
        except Exception as e:
            logger.error(sel_uo2)
            logger.error(e)
        logger.debug('max={} micro={}'.format(max,seq_micro[0][0]))
        if seq_micro[0][0]== max:
            check=1
        #exit()
    

    cur2.close()
    
    
    ##################################################################################################
    #                               CHIUDO LE CONNESSIONI
    ################################################################################################## 
    logger.info("Chiudo definitivamente le connesioni al DB")
    con.close()
    conn.close()

    # check se c_handller contiene almeno una riga 
    error_log_mail(errorfile, 'roberto.marzocchi@amiu.genova.it', os.path.basename(__file__), logger)

if __name__ == "__main__":
    main()