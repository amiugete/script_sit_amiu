#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2021
# Roberto Marzocchi

'''
Script per le attività da fare una volta prodotto etl.base_ecopunti  e verificato con l'ausilio del progetto QGIS apposito

Esegue le seguenti operazioni:

1) con l'elenco dei codici civici cerca le utenze domestiche e non domestiche su Oracle e produce due file excel

2) inserisce i dati nella cartella che serve a Laura Calvello per Saltax (etl.ecopunti)
'''


import os,sys, getopt
import inspect, os.path
# da sistemare per Linux
import cx_Oracle


import xlsxwriter


import psycopg2

import datetime

currentdir = os.path.dirname(os.path.realpath(__file__))
parentdir = os.path.dirname(currentdir)
sys.path.append(parentdir)

from credenziali import *
#from credenziali import db, port, user, pwd, host, user_mail, pwd_mail, port_mail, smtp_mail



#libreria per gestione log
import logging


#num_giorno=datetime.datetime.today().weekday()
#giorno=datetime.datetime.today().strftime('%A')

filename = inspect.getframeinfo(inspect.currentframe()).filename
path     = os.path.dirname(os.path.abspath(filename))


giorno_file=datetime.datetime.today().strftime('%Y%m%d')


logfile='{}/log/{}_ecopunti_parte2.log'.format(path, giorno_file)

logging.basicConfig(
    handlers=[logging.FileHandler(filename=logfile, encoding='utf-8', mode='a')],
    format='%(asctime)s\t%(levelname)s\t%(message)s',
    #filemode='w', # overwrite or append
    #fileencoding='utf-8',
    #filename=logfile,
    level=logging.INFO)





# Libreria per invio mail
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




def main(argv):


    logging.info('Leggo gli input')
    try:
        opts, args = getopt.getopt(argv,"hm:a:e:",["mail=", "area=", "ecopunti="])
    except getopt.GetoptError:
        logging.error('ecopunti_parte2.py  -m <mail> -a <area>')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print('ecopunti_parte2.py - m <mail> -a <area> -e <true/false>')
            sys.exit()
        elif opt in ("-m", "--mail"):
            mail = arg
            logging.info('Mail cui inviare i dati = {}'.format(mail))
        elif opt in ("-a", "--area"):
            area = arg
            logging.info('id area = {}'.format(area))
        elif opt in ("-e", "--ecopunti"):
            ecopunto = arg
            if ecopunto=='true':
                check_eco=1
            elif ecopunto=='false':
                check_eco=2
            else:
                print('ecopunti_parte2.py - m <mail> -a <area> -e <true/false>')
                sys.exit()
                
            logging.info('ecopunto = {}'.format(ecopunto))


    # carico i mezzi sul DB PostgreSQL
    logging.info('Connessione al db SIT')
    try:
        conn = psycopg2.connect(dbname=db,
                        port=port,
                        user=user,
                        password=pwd,
                        host=host)
        logging.info('Connessione riuscita')
    except Exception as e:
        logging.error(e)


    curr = conn.cursor()
    conn.autocommit = True



    logging.info('Connessione al db Saltax')
    try:
        conn_saltax = psycopg2.connect(dbname=db_saltax,
                        port=port,
                        user=user,
                        password=pwd,
                        host=host_saltax)
        logging.info('Connessione riuscita')
    except Exception as e:
        logging.error(e)
    query='''select cod_civico from etl.base_ecopunti'''
    


    try:
        curr.execute(query)
        lista_civici=curr.fetchall()
    except Exception as e:
        logging.error(e)


    #inizializzo gli array
    cod_civico=[]

           
    for vv in lista_civici:
        #logging.debug(vv[0])
        cod_civico.append(vv[0])

    curr.close()
    curr = conn.cursor()

    if check_eco==1:
        query_area='''select replace(nome,' ', '_') as nome_area from etl.aree_ecopunti where id = %s'''
    elif check_eco==2:
        query_area='''select replace(nome,' ', '_') as nome_area from etl.aree where id = %s'''
        
    
    try:
        curr.execute(query_area, (area,))
        n_area=curr.fetchall()
    except Exception as e:
        logging.error(e)
        logging.error(query_area)
        #logging.error(area)

    for aa in n_area:
        nome_area=aa[0]

    curr.close()

    if check_eco==1:
        oggetto_mail='Invio utenze ecopunti ({0})'.format(nome_area)
    elif check_eco==2:
        oggetto_mail='Invio utenze area {0}'.format(nome_area)

    logging.info('Lista civici')
    curr2 = conn.cursor()
    query2 = ''' SELECT v.nome, be.testo, be.note 
	FROM etl.base_ecopunti be 
    JOIN topo.vie v 
    ON v.id_via::integer = be.cod_strada::integer
    '''

    try:
        curr2.execute(query2)
        lista_civici2=curr2.fetchall()
    except Exception as e:
        logging.error(e)


    
    nome_file0="{0}_{1}_elenco_civici_completo.xlsx".format(giorno_file, nome_area)
    file_civici="{0}/ecopunti/{1}".format(path,nome_file0)
    
    
    workbook0 = xlsxwriter.Workbook(file_civici)
    w0 = workbook0.add_worksheet()

    w0.write(0, 0, 'id') 
    w0.write(0, 1, 'Nome_via')
    w0.write(0, 2, 'Civico')
    w0.write(0, 2, 'Note')
    i=1
    for vv in lista_civici2:
        w0.write(i, 0, i) 
        w0.write(i, 1, vv[0])
        w0.write(i, 2, vv[1])
        w0.write(i, 2, vv[2])
        i+=1
        

    workbook0.close()





    # Array con i civici neri e rossi
    i=0
    while i< len(cod_civico):
        if i == 0:
            civ= ''' ('{}' '''.format(cod_civico[i])
        else:
             civ= '''{} , '{}' '''.format(civ, cod_civico[i])
        i+=1
    civ= '''{})'''.format(civ)



    # connessione Oracle
    #cx_Oracle.init_oracle_client(lib_dir=r"C:\oracle\instantclient_19_10")
    logging.info('Connessione a DB Oracle')
    cx_Oracle.init_oracle_client()
    parametri_con='{}/{}@//{}:{}/{}'.format(user_strade,pwd_strade, host_uo,port_uo,service_uo)
    logging.debug(parametri_con)
    con = cx_Oracle.connect(parametri_con)
    logging.info("Versione ORACLE: {}".format(con.version))



    
  



    nome_file="{0}_{1}_utenze_domestiche.xlsx".format(giorno_file, nome_area)
    nome_file2="{0}_{1}_utenze_nondomestiche.xlsx".format(giorno_file,nome_area)
    nome_file3="{0}_{1}_civici_utenze_domestiche.xlsx".format(giorno_file, nome_area)
    nome_file4="{0}_{1}_civici_utenze_nondomestiche.xlsx".format(giorno_file, nome_area)
    file_domestiche="{0}/ecopunti/{1}".format(path,nome_file)
    file_nondomestiche="{0}/ecopunti/{1}".format(path,nome_file2)
    file_civdomestiche="{0}/ecopunti/{1}".format(path,nome_file3)
    file_civnondomestiche="{0}/ecopunti/{1}".format(path,nome_file4)
    


    # array che uso dopo quando devo inviare le mail
    nomi_files=[]
    files=[]


    
    nomi_files.append(nome_file0)
    files.append(file_civici)

    nomi_files.append(nome_file)
    files.append(file_domestiche)

    nomi_files.append(nome_file2)
    files.append(file_nondomestiche)

    nomi_files.append(nome_file3)
    files.append(file_civdomestiche)

    nomi_files.append(nome_file4)
    files.append(file_civnondomestiche)


    workbook = xlsxwriter.Workbook(file_domestiche)
    w = workbook.add_worksheet()



    w.write(0, 0, 'ID_UTENTE') 
    w.write(0, 1, 'PROGR_UTENZA') 
    w.write(0, 2, 'COGNOME') 
    w.write(0, 3, 'NOME')
    w.write(0, 4, 'COD_VIA') 
    w.write(0, 5, 'DESCR_VIA') 
    w.write(0, 6, 'CIVICO') 
    w.write(0, 7, 'LETTERA_CIVICO')
    w.write(0, 8, 'COLORE') 
    w.write(0, 9, 'SCALA') 
    w.write(0, 10, 'INTERNO') 
    w.write(0, 11, 'LETTERA_INTERNO')
    w.write(0, 12, 'CAP') 
    w.write(0, 13, 'UNITA_URBANISTICA') 
    w.write(0, 14, 'QUARTIERE') 
    w.write(0, 15, 'CIRCOSCRIZIONE')
    w.write(0, 16, 'ABITAZIONE_DI_RESIDENZA') 
    w.write(0, 17, 'NUM_OCCUPANTI') 
    w.write(0, 18, 'DESCR_CATEGORIA')
    w.write(0, 19, 'DESCR_UTILIZZO') 
    w.write(0, 20, 'COD_INTERNO')
    w.write(0, 21, 'Presenza dato su Saltax?')
    w.write(0, 22, 'Chiave consegnata?')


    logging.info('*****************************************************')
    logging.info('Utenze domestiche su strade')

    cur = con.cursor()
    query=''' SELECT ID_UTENTE, PROGR_UTENZA, COGNOME, NOME, COD_VIA, DESCR_VIA,
        CIVICO, LETTERA_CIVICO, COLORE, SCALA, INTERNO, LETTERA_INTERNO, CAP, 
        UNITA_URBANISTICA, QUARTIERE, CIRCOSCRIZIONE, ZONA, ABITAZIONE_DI_RESIDENZA, NUM_OCCUPANTI, DESCR_CATEGORIA, DESCR_UTILIZZO, COD_INTERNO
        FROM STRADE.UTENZE_TIA_DOMESTICHE
        WHERE COD_CIVICO in {}
        '''.format(civ)

    lista_domestiche = cur.execute(query)

    i=1
    for rr in lista_domestiche:
        j=0
        #logging.debug(len(rr))
        while j<len(rr):
            w.write(i, j, rr[j])
            j+=1
        query_saltax='''select * from ecopunti_xatlas_key 
            where pper ={0} and cod_interno = '{1}'
            and data_attivazione_utenza is not null 
            and data_cessazione_utenza is null '''.format(rr[0],rr[19])
        #print(query_saltax)
        cur_saltax= conn_saltax.cursor()
        cur_saltax.execute(query_saltax)
        presente_saltax=cur_saltax.fetchall()
        if len(presente_saltax)>0:
            w.write(i, j, 'S')
        else:
            w.write(i, j, 'N')
        j+=1
        query_saltax1='''select * from ecopunti_xatlas_key 
            where pper ={0} and cod_interno = '{1}'
            and data_attivazione_utenza is not null 
            and data_cessazione_utenza is null 
            and data_consegna is not null'''.format(rr[0],rr[19])
        #print(query_saltax1)
        cur_saltax.execute(query_saltax1)
        consegnato_saltax=cur_saltax.fetchall()
        if len(consegnato_saltax)>0:
            w.write(i, j, 'Chiave già consegnata')
        cur_saltax.close()
        j+=1
        i+=1

    cur.close()
    workbook.close()



    # civici domestiche
    workbook3 = xlsxwriter.Workbook(file_civdomestiche)
    w3 = workbook3.add_worksheet()

    w3.write(0, 0, 'COD_VIA') 
    w3.write(0, 1, 'DESCR_VIA') 
    w3.write(0, 2, 'CIVICO') 
    w3.write(0, 3, 'LETTERA_CIVICO')
    w3.write(0, 4, 'COLORE')



    cur3 = con.cursor()
    query=''' SELECT DISTINCT COD_VIA, DESCR_VIA,
        CIVICO, SUB_CIVICO, COLORE 
        FROM STRADE.UTENZE_TIA_DOMESTICHE
        WHERE COD_CIVICO in {} ORDER BY DESCR_VIA
        '''.format(civ)

    #logging.debug(query)
    lista_civdomestiche = cur3.execute(query)

    i=1
    for rr in lista_civdomestiche:
        j=0
        #logging.debug(len(rr))
        while j<len(rr):
            w3.write(i, j, rr[j])
            j+=1
        i+=1

    cur3.close()
    workbook3.close()

    logging.info('*****************************************************')
    logging.info('Utenze non domestiche su strade')
    # non domestiche
    cur2 = con.cursor()

    workbook2 = xlsxwriter.Workbook(file_nondomestiche)
    w2 = workbook2.add_worksheet()


    w2.write(0, 0, 'ID_UTENTE') 
    w2.write(0, 1, 'PROGR_UTENZA') 
    w2.write(0, 2, 'NOMINATIVO') 
    w2.write(0, 3, 'CFISC_PARIVA')
    w2.write(0, 4, 'COD_VIA') 
    w2.write(0, 5, 'DESCR_VIA') 
    w2.write(0, 6, 'CIVICO') 
    #w2.write(0, 7, 'SUB_CIVICO')
    w2.write(0, 7, 'COLORE') 
    w2.write(0, 8, 'SCALA') 
    #w2.write(0, 10, 'PIANO') 
    w2.write(0, 9, 'INTERNO')
    w2.write(0, 10, 'LETTERA_INTERNO')
    w2.write(0, 11, 'CAP') 
    w2.write(0, 12, 'UNITA_URBANISTICA') 
    w2.write(0, 13, 'QUARTIERE') 
    w2.write(0, 14, 'CIRCOSCRIZIONE')
    w2.write(0, 15, 'SUPERFICIE') 
    w2.write(0, 16, 'DESCR_CATEGORIA')
    w2.write(0, 17, 'DESCR_UTILIZZO')
    w2.write(0, 18, 'COD_INTERNO')
    w2.write(0, 19, 'Presenza dato su Saltax?')
    w2.write(0, 20, 'Chiave consegnata?')
    



    query='''SELECT ID_UTENTE, PROGR_UTENZA, NOMINATIVO, CFISC_PARIVA, COD_VIA, DESCR_VIA,
CIVICO, COLORE, SCALA, INTERNO, LETTERA_INTERNO, CAP, 
UNITA_URBANISTICA, QUARTIERE, CIRCOSCRIZIONE,  SUPERFICIE, DESCR_CATEGORIA, DESCR_UTILIZZO, COD_INTERNO
FROM STRADE.UTENZE_TIA_NON_DOMESTICHE
WHERE COD_CIVICO in {}
        '''.format(civ)

    lista_nondomestiche = cur2.execute(query)

    i=1
    for rr in lista_nondomestiche:
        j=0
        #logging.debug(len(rr))
        while j<len(rr):
            w2.write(i, j, rr[j])
            j+=1
        query_saltax='''select * from ecopunti_xatlas_key 
            where pper ={0} and cod_interno = '{1}'
            and data_attivazione_utenza is not null 
            and data_cessazione_utenza is null '''.format(rr[0], rr[18])
        #print(query_saltax)
        cur_saltax= conn_saltax.cursor()
        cur_saltax.execute(query_saltax)
        presente_saltax=cur_saltax.fetchall()
        if len(presente_saltax)>0:
            w2.write(i, j, 'S')
        else:
            w2.write(i, j, 'N')
        j+=1
        query_saltax1='''select * from ecopunti_xatlas_key 
            where pper ={0} and cod_interno = '{1}'
            and data_attivazione_utenza is not null 
            and data_cessazione_utenza is null 
            and data_consegna is not null'''.format(rr[0],rr[18])
        #print(query_saltax1)
        cur_saltax.execute(query_saltax1)
        consegnato_saltax=cur_saltax.fetchall()
        if len(consegnato_saltax)>0:
            w2.write(i, j, 'Chiave già consegnata')
        cur_saltax.close()
        i+=1

    cur2.close()
    workbook2.close()



    # civici  non domestiche
    workbook4 = xlsxwriter.Workbook(file_civnondomestiche)
    w4 = workbook4.add_worksheet()

    w4.write(0, 0, 'COD_VIA') 
    w4.write(0, 1, 'DESCR_VIA') 
    w4.write(0, 2, 'CIVICO') 
    w4.write(0, 3, 'LETTERA_CIVICO')
    w4.write(0, 4, 'COLORE')



    cur4 = con.cursor()
    query=''' SELECT DISTINCT COD_VIA, DESCR_VIA,
        CIVICO, LETTERA_CIVICO, COLORE 
        FROM STRADE.UTENZE_TIA_NON_DOMESTICHE
        WHERE COD_CIVICO in {} ORDER BY DESCR_VIA
        '''.format(civ)

    #logging.debug(query)
    lista_civnondomestiche = cur4.execute(query)

    i=1
    for rr in lista_civnondomestiche:
        j=0
        #logging.debug(len(rr))
        while j<len(rr):
            w4.write(i, j, rr[j])
            j+=1
        i+=1

    cur4.close()
    workbook4.close()





    ###########################
    # Invio mail 
    ###########################

    logging.info("Invio mail")



    # Create a secure SSL context
    context = ssl.create_default_context()



   # messaggio='Test invio messaggio'


    
    #sender_email = user_mail
    receiver_email=mail
    debug_email='assterritorio@amiu.genova.it'
    #assterritorio@amiu.genova.it
    #debug_email='roberto.marzocchi@amiu.genova.it'

    body = '''Mail automatica con l'invio delle utenze degli ecopunti.<br>
    L'applicativo che gestisce l'estrazione delle utenze è stato realizzato dal gruppo <i>APPTE (SIGT)</i>.<br> 
    Segnalare tempestivamente eventuali malfunzionamenti inoltrando la presente mail a {}<br><br>
    Giorno {}<br><br>
    <i>AMIU Assistenza Territorio</i>
    '''.format(debug_email, datetime.datetime.today().strftime('%d/%m/%Y'))
    


    # Create a multipart message and set headers
    message = MIMEMultipart()
    message["From"] = sender_email
    message["To"] = receiver_email
    message["Cc"] = debug_email
    message["Subject"] = oggetto_mail
    #message["Bcc"] = debug_email  # Recommended for mass emails
    message.preamble = "File con le utenze"

    # Add body to email
    message.attach(MIMEText(body, "html"))

    #filename = file_variazioni  # In same directory as script

    #aggiungo logo 
    logoname='{}/img/logo_amiu.jpg'.format(path)
    immagine(message,logoname)


    i=0
    while i < len(files):
        ctype, encoding = mimetypes.guess_type(files[i])
        if ctype is None or encoding is not None:
            ctype = "application/octet-stream"

        maintype, subtype = ctype.split("/", 1)

        if maintype == "text":
            fp = open(files[i])
            # Note: we should handle calculating the charset
            attachment = MIMEText(fp.read(), _subtype=subtype)
            fp.close()
        elif maintype == "image":
            fp = open(files[i], "rb")
            attachment = MIMEImage(fp.read(), _subtype=subtype)
            fp.close()
        elif maintype == "audio":
            fp = open(files[i], "rb")
            attachment = MIMEAudio(fp.read(), _subtype=subtype)
            fp.close()
        else:
            fp = open(files[i], "rb")
            attachment = MIMEBase(maintype, subtype)
            attachment.set_payload(fp.read())
            fp.close()
            encoders.encode_base64(attachment)
        attachment.add_header("Content-Disposition", "attachment", filename=nomi_files[i])
        message.attach(attachment)
        i+=1

    '''
    # Open PDF file in binary mode
    with open(filename, "rb") as attachment:
        # Add file as application/octet-stream
        # Email client can usually download this automatically as attachment
        part = MIMEBase("application", "octet-stream")
        part.set_payload(attachment.read())


    # Encode file in ASCII characters to send by email    
    encoders.encode_base64(part)

    # Add header as key/value pair to attachment part
    part.add_header(
        "Content-Disposition",
        f"attachment; filename= {nome_file}",
    )

    # Add attachment to message and convert message to string
    message.attach(part)
    '''
    
    
    text = message.as_string()

    
    logging.info("Richiamo la funzione per inviare mail")
    invio=invio_messaggio(message)
    logging.info(invio)

    logging.info("Mail inviata a {} e a nostro indirizzo".format(receiver_email))
    


    logging.info("CHIUSURA CONNESSIONI DB APERTE")
    conn.close()
    con.close()
    conn_saltax.close()
    
    exit()

    
    i=0
    #k=1
    cont=0
    logging.info('*****************************************************')
    logging.info('Utenze domestiche su strade')
    while i< len(cod_civico):
        query='''
       SELECT ID_UTENTE, PROGR_UTENZA, COGNOME, NOME, COD_VIA, DESCR_VIA,
CIVICO, SUB_CIVICO, COLORE, SCALA, PIANO, INTERNO, CAP, 
UNITA_URBANISTICA, QUARTIERE, CIRCOSCRIZIONE, ZONA, ABITAZIONE_DI_RESIDENZA,  DESCR_CATEGORIA, DESCR_UTILIZZO
FROM STRADE.UTENZE_TIA_DOMESTICHE
WHERE COD_CIVICO = '{}' '''.format(cod_civico[i])

       
        

        #logging.debug(query)
        lista_domestiche = cur.execute(query)
        #cur.execute('select * from all_tables')
        #k=0
        cc=0
        for r in lista_domestiche:
            #if result[7] < = '':
            j=0
            while j<len(r):
                logging.debug((cont+j+1))
                w.write((cont+cc+1), j, r[j])
                j+=1
            cc+=1
        cont=cont+cc
        i+=1



    cur.close()
    workbook.close()


    cur2 = con.cursor()

    workbook2 = xlsxwriter.Workbook(file_nondomestiche)
    w2 = workbook2.add_worksheet()


    w2.write(0, 0, 'ID_UTENTE') 
    w2.write(0, 1, 'PROGR_UTENZA') 
    w2.write(0, 2, 'NOMINATIVO') 
    w2.write(0, 3, 'CFISC_PARIVA')
    w2.write(0, 4, 'COD_VIA') 
    w2.write(0, 5, 'DESCR_VIA') 
    w2.write(0, 6, 'CIVICO') 
    #w2.write(0, 7, 'SUB_CIVICO')
    w2.write(0, 8, 'COLORE') 
    w2.write(0, 9, 'SCALA') 
    #w2.write(0, 10, 'PIANO') 
    w2.write(0, 11, 'INTERNO')
    w2.write(0, 11, 'LETTERA_INTERNO')
    w2.write(0, 12, 'CAP') 
    w2.write(0, 13, 'UNITA_URBANISTICA') 
    w2.write(0, 14, 'QUARTIERE') 
    w2.write(0, 15, 'CIRCOSCRIZIONE')
    w2.write(0, 16, 'ABITAZIONE_DI_RESIDENZA') 
    w2.write(0, 17, 'DESCR_CATEGORIA')
    w2.write(0, 18, 'DESCR_UTILIZZO') 


    i=0
    #k=1
    cont=0
    logging.info('*****************************************************')
    logging.info('Utenze non domestiche su strade')
    while i< len(cod_civico):
        query='''
       SELECT ID_UTENTE, PROGR_UTENZA, NOMINATIVO, CFISC_PARIVA, COD_VIA, DESCR_VIA,
CIVICO, COLORE, SCALA, INTERNO, LETTERA_INTERNO, CAP, 
UNITA_URBANISTICA, QUARTIERE, CIRCOSCRIZIONE, ZONA, SUPERFICIE, NUM_OCCUPANTI, ABITAZIONE_DI_RESIDENZA,  DESCR_CATEGORIA, DESCR_UTILIZZO
FROM STRADE.UTENZE_TIA_NON_DOMESTICHE
WHERE COD_CIVICO = '{}' '''.format(cod_civico[i])
        #logging.debug(query)
        lista_domestiche = cur2.execute(query)
        #cur.execute('select * from all_tables')
        #k=0
        cc=0
        for r in lista_domestiche:
            #if result[7] < = '':
            j=0
            while j<len(r):
                logging.debug((cont+j+1))
                w2.write((cont+cc+1), j, r[j])
                j+=1
            cc+=1
        cont=cont+cc
        i+=1




        '''except:
            logging.warning('Civico {} non ha utenze domestiche'.format(cod_civico[i]))
        '''
            
        '''w.write((i+1), 1, r[1]) 
        w.write((i+1), 2, r[2]) 
        w.write((i+1), 3, r[3])
        w.write((i+1), 4, r[4]) 
        w.write((i+1), 5, r[5]) 
        w.write((i+1), 6, r[6]) 
        w.write((i+1), 7, r[7])
        w.write((i+1), 8, r'COLORE') 
        w.write((i+1), 9, 'SCALA') 
        w.write((i+1), 10, 'PIANO') 
        w.write((i+1), 11, 'INTERNO')
        w.write((i+1), 12, 'CAP') 
        w.write((i+1), 13, 'UNITA_URBANISTICA') 
        w.write((i+1), 14, 'QUARTIERE') 
        w.write((i+1), 15, 'CIRCOSCRIZIONE')
        w.write((i+1), 16, 'ABITAZIONE_DI_RESIDENZA') 
        w.write((i+1), 17, 'DESCR_CATEGORIA')
        w.write((i+1), 18, 'DESCR_UTILIZZO')''' 
        #k+=1
        i+=1
            
    
    cur2.close()
    workbook2.close()



if __name__ == "__main__":
    main(sys.argv[1:])