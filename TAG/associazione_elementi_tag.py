#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2025
# Roberto Marzocchi, Roberta Fagandini

'''
Scopo dello script è provare a associare i TAG letti da tellus agli elementi presenti su SIT



'''

#from msilib import type_short
import os, sys, re  # ,shutil,glob
import inspect







from datetime import date, datetime, timedelta



import psycopg2



currentdir = os.path.dirname(os.path.realpath(__file__))
parentdir = os.path.dirname(currentdir)
sys.path.append(parentdir)
from credenziali import *



import logging





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



    

def main():
    
    logger.info('Il PID corrente è {0}'.format(os.getpid()))

    
    # connessione a SIT
    nome_db=db
    logger.info('Connessione al db {}'.format(nome_db))
    conn = psycopg2.connect(dbname=nome_db,
                        port=port,
                        user=user,
                        password=pwd,
                        host=host)


    curr = conn.cursor()
    
    
    refresh_mv='''refresh materialized view tellus.mv_tag_mediati'''
    
    try:
        curr.execute(refresh_mv)
    except Exception as e:
        logger.error(refresh_mv)
        logger.error(e)
    
    conn.commit()
    curr.close()
    
    
    
    curr = conn.cursor()
        
    select_elementi = '''select e.id_elemento,
p.id as id_piazzola,
te.descrizione, 
tr.nome as nome_rifiuto,
case
	when tr.tipo_rifiuto = 1 then 'S'
	when tr.tipo_rifiuto = 2 then 'V'
	when tr.tipo_rifiuto in (3,7) then 'C'
	when tr.tipo_rifiuto = 4 then 'M'
	when tr.tipo_rifiuto = 10 then 'P'
	when tr.tipo_rifiuto = 5 then 'O'
end  lettera,
te.volume,
e.tag,
st_transform(st_buffer(p.geoloc,11),4326) as geom
from elem.elementi e
join elem.tipi_elemento te on te.tipo_elemento = e.tipo_elemento 
join elem.tipi_rifiuto tr on tr.tipo_rifiuto = te.tipo_rifiuto
join geo.piazzola p on e.id_piazzola = p.id
join elem.piazzole p2 on p2.id_piazzola= e.id_piazzola
where te.tipologia_elemento = 'P'
and p2.data_eliminazione is null
/*order by 2,1*/'''



    try:
        curr.execute(select_elementi)
        elementi=curr.fetchall()
    except Exception as e:
        logger.error(select_elementi)
        logger.error(e)


    curr1 = conn.cursor()
    curr2 = conn.cursor()
    for e in elementi:
        
        select_tag='''select * from tellus.mv_tag_mediati
        WHERE  
        /*intereseca il buffer*/
        ST_Intersects(%s, geom)
        /* frazione */
        AND  fraz ilike %s
        /*volume*/
        AND volume = %s
        /* elemento non ha già tag associato */
        AND tag not in (SELECT tag FROM tellus.elementi_tag_wip) 
        LIMIT 1'''

        try:
            curr1.execute(select_tag, (e[7], e[4], e[5],))
            tag=curr1.fetchall()
        except Exception as e:
            check_error=1
            logger.error(select_tag)
            logger.error(e)
        
        if len(tag)==1:
            for t in tag:
                query_insert = '''INSERT INTO tellus.elementi_tag_wip (tag, id_elemento) VALUES (%s, %s)'''
                #logger.debug(f'Faccio insert di tag {t[0]} in elemento {e[0]}')
                try:
                    curr2.execute(query_insert, (t[0], e[0],))
                except Exception as e:
                    logger.error(query_insert)
                    logger.error(e)
            #exit()
        conn.commit()
    
    
    
    # check se c_handller contiene almeno una riga 
    error_log_mail(errorfile, 'assterritorio@amiu.genova.it', os.path.basename(__file__), logger)
    
    
    logger.info("chiudo le connessioni in maniera definitiva")
    curr2.close()
    curr1.close()
    curr.close()
    conn.close()
    
    
    
if __name__ == "__main__":
    main()      