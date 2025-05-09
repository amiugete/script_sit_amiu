#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2025
# Roberto Marzocchi

'''
Lo script analizza le ispezioni sovrariempimenti fatte e prepara una mail per le UT con le problematiche riscontrate 

 
'''

import os, sys, re  # ,shutil,glob
import inspect, os.path

#import getopt  # per gestire gli input

#import pymssql

import psycopg2

from workalendar.europe import Italy


from credenziali import *


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

import json
from PIL import Image, ExifTags
from datetime import datetime

#LOG

filename = inspect.getframeinfo(inspect.currentframe()).filename
path     = os.path.dirname(os.path.abspath(filename))


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







def main():
    logger.info('Il PID corrente è {0}'.format(os.getpid()))
    # carico i mezzi sul DB PostgreSQL
    logger.info('Connessione al db')
    conn = psycopg2.connect(dbname=db,
                        port=port,
                        user=user,
                        password=pwd,
                        host=host)

    curr = conn.cursor()
    #conn.autocommit = True
    
    query_anomalie='''
    select
ispezione, 
id_piazzola, 
comune,
municipio, 
via,
civ, riferimento, 
elementi_presenti_sit, 
string_agg(concat(num_elementi, ' x ', tipo_elemento), ', ') as elementi_non_trovati_descr, 
string_agg(id_elementi_assenti, ', ') as elementi_non_trovati,
ut, mail
from (
	select 
	string_agg(distinct concat('', lpad(i.id::text, 4, '0'), ' del ', to_char(data_ora, 'DD/MM/YYYY'), ' a cura di ', ispettore),', ') as ispezione,
	vpd.id_piazzola, vpd.comune, vpd.municipio, vpd.via, vpd.civ, vpd.riferimento,
	vpd.elementi as elementi_presenti_sit,
	string_agg(e0.id_elemento::text, ', ') as id_elementi_assenti, 
	count(e0.id_elemento) as num_elementi,
	concat(tr.nome, ' (', te.nome_stampa,') <font color="red">Id da eliminare: ', string_agg(e0.id_elemento::text, ', '), '</font>') as tipo_elemento, 
	u.descrizione as ut, 
	u.mail
	from elem.elementi e0 
	join elem.v_piazzole_dwh vpd on vpd.id_piazzola = e0.id_piazzola
	join elem.piazzole p on p.id_piazzola = e0.id_piazzola
	join elem.aste a on a.id_asta = p.id_asta
	join topo.ut u on a.id_ut = u.id_ut
	join elem.tipi_elemento te on te.tipo_elemento = e0.tipo_elemento
	JOIN elem.tipi_rifiuto tr ON te.tipo_rifiuto = tr.tipo_rifiuto
	left join sovrariempimenti.ispezioni i on i.id_piazzola =e0.id_piazzola
	where e0.id_piazzola in (
		select distinct id_piazzola from sovrariempimenti.ispezione_elementi ie
		join elem.elementi e on e.id_elemento = ie.id_elemento
	) and e0.id_elemento not in (
		select distinct id_elemento from sovrariempimenti.ispezione_elementi ie1
	) 
	and e0.tipo_elemento not in (101,198,180) 
	and te.tipologia_elemento in ('P', 'L', 'C')
	group by 
	vpd.id_piazzola, vpd.comune, vpd.municipio, vpd.via, vpd.civ, vpd.riferimento,vpd.elementi,
	tr.nome, te.nome_stampa,
	u.descrizione, u.mail
) anomal
group by ispezione, 
id_piazzola, 
comune,
municipio, 
via,
civ, riferimento, 
elementi_presenti_sit, ut, mail
order by ispezione, id_piazzola
    '''
    
    
    try:
        curr.execute(query_anomalie)
        lista_anomalie=curr.fetchall()
    except Exception as e:
        logger.error(e)


    

           
    for aa in lista_anomalie:
        
        check_foto=1
        try:
            filename_foto='{}/{}.jpg'.format(path_foto, aa[1])
            image_exif = Image.open(filename_foto)._getexif()
            if image_exif:
                # Make a map with tag names
                exif = { ExifTags.TAGS[k]: v for k, v in image_exif.items() if k in ExifTags.TAGS and type(v) is not bytes }
                # Grab the date
                date_obj = datetime.strptime(exif['DateTimeOriginal'], '%Y:%m:%d %H:%M:%S')
                logger.info(date_obj)
                testo_foto='''In allegato la foto del {}'''.format(date_obj.strftime("%d/%m/%Y %H:%M"))
            else:
                print('Unable to get date from exif for %s' % filename)
        except Exception as e:
            logger.warning(e)
            testo_foto='Nessuna foto disponibile'
            check_foto=0
        
        testo_mail = '''Buongiorno {0} ({9}), <br><br>
        
        sulla base dell'ispezione {1} sulla piazzola {2} di {3},  {4}, Rif. {5} 
        sono state riscontrate le seguenti anomalie rispetto a quanto presente su SIT: <br>
        
        <ul>
        <li> <b>Elementi presenti su SIT:</b><br>{6}</li>
        <li> <b>Elementi non trovati:</b><br>{7}</li> 
        </ul>
        
        Per modificare la piazzola su SIT 
        <a href="https://amiupostgres.amiu.genova.it/SIT/#!/home/edit-piazzola/{2}/"> clicca qua </a>. 
        <font color="red"> Prestare attenzione agli id degli elementi da eliminare. </font> 
        <br><br>
        
        Per segnalare delle problematiche sull'ispezione contatta chi l'ha eseguita al più presto.
        
        <br><br>
        
        {8}
        <br><br>
        AMIU<br>
        <img src="cid:image1" alt="Logo" width=197>
        <br>Questa mail è stata creata in automatico. 
        In caso di dubbi contattare i referenti delle ispezioni 
        
        '''.format(aa[10], aa[0], aa[1], aa[4], aa[5], aa[6],
                   aa[7].replace(',','<br>'), aa[8].replace(',','<br>'),
                   testo_foto, aa[11])
        
        logger.debug(testo_mail)
        
        subject = "Alert piazzola {} - Ispezione {}".format(aa[1], aa[0])
            
        ##sender_email = user_mail
        receiver_email='assterritorio@amiu.genova.it'
        debug_email='roberto.marzocchi@amiu.genova.it'

        to_mail= 'roberto.longo@amiu.genova.it, Marco.Martina@amiu.genova.it'
    
        # Create a multipart message and set headers
        message = MIMEMultipart()
        message["From"] = 'noreply@amiu.genova.it'
        message["To"] = to_mail
        message["Bcc"] = receiver_email
        #message["CCn"] = debug_email
        message["Subject"] = subject
        #message["Bcc"] = debug_email  # Recommended for mass emails
        message.preamble = "Chiusura schede di lavoro"


                            
        # Add body to email
        message.attach(MIMEText(testo_mail, "html"))


        #aggiungo logo 
        logoname='{}/img/logo_amiu.jpg'.format(path)
        immagine(message,logoname)
        if check_foto==1:
            allegato(message,filename_foto, '{}.jpg'.format(aa[1]))

        
        
        text = message.as_string()

        logger.info("Richiamo la funzione per inviare mail")
        invio=invio_messaggio(message)
        logger.info(invio)
  
        
        
    error_log_mail(errorfile, 'assterritorio@amiu.genova.it', os.path.basename(__file__), logger)
    logger.info("chiudo le connessioni in maniera definitiva")
        
    
if __name__ == "__main__":
    main()   