#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2021
# Roberto Marzocchi

'''
Script sulla falsariga di quello per gli ecopunti 
non parte da alberghi.base_ecopunti, ma direttamente dai codici via che vanno variati di volta in volta

Esegue le seguenti operazioni:

1) con l'elenco dei codici civici cerca le utenze domestiche e non domestiche su Oracle e produce due file excel

'''


#codici_via= '40500, 19420, 61980, 49860'

#file_csv ='elenco_vie_test2.txt'
#prefisso1 ='zona valpolcevera'
#mail = 'roberto.marzocchi@gmail.com'






import os,sys, getopt
import inspect, os.path
# da sistemare per Linux
import cx_Oracle


import xlsxwriter


import psycopg2

import datetime
import csv


currentdir = os.path.dirname(os.path.realpath(__file__))
parentdir = os.path.dirname(currentdir)
sys.path.append(parentdir)

from credenziali import *
#from credenziali import db, port, user, pwd, host, user_mail, pwd_mail, port_mail, smtp_mail



#libreria per gestione log
import logging


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




def main(argv):
    #num_giorno=datetime.datetime.today().weekday()
    #giorno=datetime.datetime.today().strftime('%A')

    filename = inspect.getframeinfo(inspect.currentframe()).filename
    path     = os.path.dirname(os.path.abspath(filename))


    giorno_file=datetime.datetime.today().strftime('%Y%m%d')

    #giorno_file='{}_{}'.format(giorno_file, prefisso1.replace(' ', '_'))

    logfile='{}/log/{}_utenze.log'.format(path, giorno_file)

    logging.basicConfig(
        handlers=[logging.FileHandler(filename=logfile, encoding='utf-8', mode='a')],
        format='%(asctime)s\t%(levelname)s\t%(message)s',
        #filemode='w', # overwrite or append
        #fileencoding='utf-8',
        #filename=logfile,
        level=logging.DEBUG)






 
    logging.info('Leggo gli input')
    try:
        opts, args = getopt.getopt(argv,"hi:p:m:",["ifile=","prefix=", "mail="])
    except getopt.GetoptError:
        logging.error('seleziona_utenze_vie.py -i <inputfile> -p <prefisso> -m <mail>')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print('seleziona_utenze_vie.py -i <inputfile> -o <outputfile>')
            sys.exit()
        elif opt in ("-i", "--ifile"):
            file_csv = arg
            logging.info('Input file = {}'.format(file_csv))
        elif opt in ("-p", "--prefix"):
            prefisso1 = arg
            logging.info('Prefisso file = {}'.format(prefisso1))
        elif opt in ("-m", "--mail"):
            mail = arg
            logging.info('Mail cui inviare i dati = {}'.format(mail))


    #aggiorno il prefisso del file
    giorno_file='{}_{}'.format(giorno_file, prefisso1.replace(' ', '_'))
    
    # carico i mezzi sul DB PostgreSQL
    logging.info('Leggo il file CSV')

    with open(file_csv) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        line_count = 0
        for row in csv_reader:
            if line_count == 0:
                logging.debug(f'Column names are {", ".join(row)}')
                line_count += 1
            elif line_count==1:
                codici_via = '{}'.format(row[0])
                line_count += 1
            else: 
                codici_via = '{}, {}'.format(codici_via, row[0])
                line_count += 1
        logging.debug(f'Processed {line_count-1} lines.')
        logging.debug(codici_via)

    #exit()
    

    logging.info('Connessione al db')
    conn = psycopg2.connect(dbname=db,
                        port=port,
                        user=user,
                        password=pwd,
                        host=host)

    curr = conn.cursor()
    conn.autocommit = True

    
    query='''select n.cod_civico from geo.civici_neri n 
where cod_strada::integer in ({0}) 
union 
select n.cod_civico from geo.civici_rossi n 
where cod_strada::integer in ({0})'''.format(codici_via)
    


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


    logging.info('Lista civici')
    curr2 = conn.cursor()
    query2 = ''' SELECT v.nome, be.testo FROM
(select n.testo, n.cod_strada from geo.civici_neri n 
where cod_strada::integer in ({0}) 
union 
select n.testo, n.cod_strada from geo.civici_rossi n 
where cod_strada::integer in ({0})) as be
join  topo.vie v 
ON v.id_via::integer = be.cod_strada::integer'''.format(codici_via)

    try:
        curr2.execute(query2)
        lista_civici2=curr2.fetchall()
    except Exception as e:
        logging.error(e)


    # array che uso dopo quando devo inviare le mail
    nomi_files=[]
    files=[]

    nomi_files.append('elenco_vie.txt')
    #files.append('/var/www/html/utenze/file/elenco_vie.txt')
    files.append(file_csv)

    nome_file0="{0}_elenco_civici_completo.xlsx".format(giorno_file)
    file_civici="{0}/utenze/{1}".format(path,nome_file0)
    
    nomi_files.append(nome_file0)
    files.append(file_civici)

    
    workbook0 = xlsxwriter.Workbook(file_civici)
    w0 = workbook0.add_worksheet()

    w0.write(0, 0, 'id') 
    w0.write(0, 1, 'Nome_via')
    w0.write(0, 2, 'Civico')
    i=1
    for vv in lista_civici2:
        w0.write(i, 0, i) 
        w0.write(i, 1, vv[0])
        w0.write(i, 2, vv[1])
        i+=1
        

    workbook0.close()







    logging.info("Tentativo connessione ORACLE")
    # connessione Oracle
    #cx_Oracle.init_oracle_client(lib_dir=r"C:\oracle\instantclient_19_10")
    cx_Oracle.init_oracle_client()
    parametri_con='{}/{}@//{}:{}/{}'.format(user_strade,pwd_strade, host_uo,port_uo,service_uo)
    logging.debug(parametri_con)
    con = cx_Oracle.connect(parametri_con)
    logging.info("Versione ORACLE: {}".format(con.version))



    
  



    nome_file="{0}_utenze_domestiche.xlsx".format(giorno_file)
    nome_file2="{0}_utenze_nondomestiche.xlsx".format(giorno_file)
    nome_file3="{0}_civici_utenze_domestiche.xlsx".format(giorno_file)
    nome_file4="{0}_civici_utenze_nondomestiche.xlsx".format(giorno_file)
    file_domestiche="{0}/utenze/{1}".format(path,nome_file)
    file_nondomestiche="{0}/utenze/{1}".format(path,nome_file2)
    file_civdomestiche="{0}/utenze/{1}".format(path,nome_file3)
    file_civnondomestiche="{0}/utenze/{1}".format(path,nome_file4)


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
    w.write(0, 10, 'PIANO') 
    w.write(0, 11, 'INTERNO')
    w.write(0, 12, 'CAP') 
    w.write(0, 13, 'UNITA_URBANISTICA') 
    w.write(0, 14, 'QUARTIERE') 
    w.write(0, 15, 'CIRCOSCRIZIONE')
    w.write(0, 16, 'ZONA') 
    w.write(0, 17, 'ABITAZIONE_DI_RESIDENZA') 
    w.write(0, 18, 'DESCR_CATEGORIA')
    w.write(0, 19, 'DESCR_UTILIZZO') 



    logging.info('*****************************************************')
    logging.info('Utenze domestiche su strade')

    cur = con.cursor()
    query=''' SELECT ID_UTENTE, PROGR_UTENZA, COGNOME, NOME, COD_VIA, DESCR_VIA,
        CIVICO, SUB_CIVICO, COLORE, SCALA, PIANO, INTERNO, CAP, 
        UNITA_URBANISTICA, QUARTIERE, CIRCOSCRIZIONE, ZONA, ABITAZIONE_DI_RESIDENZA,  DESCR_CATEGORIA, DESCR_UTILIZZO
        FROM STRADE.UTENZE_TIA_DOMESTICHE
        WHERE COD_VIA in ({})
        '''.format(codici_via)

    try: 
        lista_domestiche = cur.execute(query)
    except Exception as e:
        logging.error(query)
        logging.error(e)
        exit()
    

    i=1
    for rr in lista_domestiche:
        j=0
        #logging.debug(len(rr))
        while j<len(rr):
            w.write(i, j, rr[j])
            j+=1
        i+=1

    cur.close()
    workbook.close()


    logging.info('*****************************************************')
    logging.info('Civici Utenze domestiche su strade')
    # civici domestiche
    workbook3 = xlsxwriter.Workbook(file_civdomestiche)
    w3 = workbook3.add_worksheet()

    w3.write(0, 0, 'COD_VIA') 
    w3.write(0, 1, 'DESCR_VIA') 
    w3.write(0, 2, 'CIVICO') 
    w3.write(0, 3, 'LETTERA_CIVICO')
    w3.write(0, 4, 'COLORE')



    cur3 = con.cursor()
    query='''SELECT DISTINCT COD_VIA, DESCR_VIA,
        CIVICO, SUB_CIVICO, COLORE 
        FROM STRADE.UTENZE_TIA_DOMESTICHE
        WHERE COD_VIA in ({}) ORDER BY DESCR_VIA
        '''.format(codici_via) 
        

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
    logging.info('Uenze non domestiche su strade')
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
    w2.write(0, 15, 'ZONA')
    w2.write(0, 16, 'SUPERFICIE')
    w2.write(0, 17, 'NUM_OCCUPANTI')
    w2.write(0, 18, 'ABITAZIONE_DI_RESIDENZA') 
    w2.write(0, 19, 'DESCR_CATEGORIA')
    w2.write(0, 20, 'DESCR_UTILIZZO')



    query='''SELECT ID_UTENTE, PROGR_UTENZA, NOMINATIVO, CFISC_PARIVA, COD_VIA, DESCR_VIA,
CIVICO, COLORE, SCALA, INTERNO, LETTERA_INTERNO, CAP, 
UNITA_URBANISTICA, QUARTIERE, CIRCOSCRIZIONE, ZONA, SUPERFICIE, NUM_OCCUPANTI, ABITAZIONE_DI_RESIDENZA,  DESCR_CATEGORIA, DESCR_UTILIZZO
FROM STRADE.UTENZE_TIA_NON_DOMESTICHE
WHERE COD_VIA in ({})
        '''.format(codici_via)

    lista_nondomestiche = cur2.execute(query)

    i=1
    for rr in lista_nondomestiche:
        j=0
        #logging.debug(len(rr))
        while j<len(rr):
            w2.write(i, j, rr[j])
            j+=1
        i+=1

    cur2.close()
    workbook2.close()


    logging.info('*****************************************************')
    logging.info('Civici Utenze Non domestiche su strade')
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
        WHERE COD_VIA in ({}) ORDER BY DESCR_VIA
        '''.format(codici_via)

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


    subject = "Invio utenze zona {}".format(prefisso1)
    
    sender_email = user_mail
    receiver_email=mail
    #debug_email='roberto.marzocchi@amiu.genova.it'
    debug_email='assterritorio@amiu.genova.it'
    #assterritorio@amiu.genova.it


    body = '''Mail automatica con l'invio delle utenze.\n
    L'applicativo che gestisce l'estrazione delle utenze è stato realizzato dal gruppo GETE.\n 
    Segnalare tempestivamente eventuali malfunzionamenti inoltrando la presente mail a {}\n\n
    Giorno {}\n\n
    AMIU Assistenza Territorio
    '''.format(debug_email, datetime.datetime.today().strftime('%d/%m/%Y'))
    


    # Create a multipart message and set headers
    message = MIMEMultipart()
    message["From"] = sender_email
    message["To"] = receiver_email
    message["Cc"] = debug_email
    message["Subject"] = subject
    #message["Bcc"] = debug_email  # Recommended for mass emails
    message.preamble = "File con le utenze"

    # Add body to email
    message.attach(MIMEText(body, "plain"))

    #filename = file_variazioni  # In same directory as script


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






    '''with smtplib.SMTP_SSL(smtp_mail, port_mail, context=context) as server:
        server.login(user_mail, pwd_mail)
        server.sendmail(user_mail, receiver_email, text)
        # TODO: Send email here

    '''
    # Now send or store the message
    with smtplib.SMTP_SSL(smtp_mail, port_mail, context=context) as s:
        s.login(user_mail, pwd_mail)
        s.send_message(message)

    logging.info("Mail inviata a {} e a nostro indirizzo".format(receiver_email))


if __name__ == "__main__":
    main(sys.argv[1:])