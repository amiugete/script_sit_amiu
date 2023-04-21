#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2021
# Roberto Marzocchi

'''
Lo script interroga i WS di IDEA pdrAlbero:

- verifica se ci sono nuovi contenitori
- verifica se ci sono contenitori da rimuovere

Manda mail a assterritorio

'''


import os, sys, getopt, re  # ,shutil,glob
import requests
from requests.exceptions import HTTPError




import json


import inspect, os.path




import psycopg2
import sqlite3


currentdir = os.path.dirname(os.path.realpath(__file__))
parentdir = os.path.dirname(currentdir)
sys.path.append(parentdir)

sys.path.append('../')
from credenziali import *
from recupera_token import *

#import requests
import datetime

import logging

filename = inspect.getframeinfo(inspect.currentframe()).filename
path = os.path.dirname(os.path.abspath(filename))

giorno_file=datetime.datetime.today().strftime('%Y%m%d%H%M')



#tmpfolder=tempfile.gettempdir() # get the current temporary directory
logfile='{}/accoppiamento_sit_idea.log'.format(path)
errorfile='{}/error_accoppiamento_sit_idea.log'.format(path)
#if os.path.exists(logfile):
#    os.remove(logfile)

'''logging.basicConfig(
    #handlers=[logging.FileHandler(filename=logfile, encoding='utf-8', mode='w')],
    format='%(asctime)s\t%(levelname)s\t%(message)s',
    #filemode='w', # overwrite or append
    #fileencoding='utf-8',
    #filename=logfile,
    level=logging.DEBUG)
'''


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


# Creare Excel per invio piazzole nuove installate e censite
import xlsxwriter


# MAIL - libreria per invio mail
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



def distinct_list(seq): # Order preserving
  ''' Modified version of Dave Kirby solution '''
  seen = set()
  return [x for x in seq if x not in seen and not seen.add(x)]


def contenitori_piazzola(conn, logger, id_piazzola):
    curr = conn.cursor()
    logger.debug(id_piazzola)
    select_cont = '''select id_piazzola, 
                string_agg(id_elemento_idea, ', ') as id_elementi, 
                string_agg(targa_contenitore , ', ') as targhe
                from idea.censimento_idea ci 
                where id_piazzola = %s
                group by id_piazzola '''
    try:
        curr.execute(select_cont, (id_piazzola,))
        lista_cont=curr.fetchall()
    except Exception as e:
        logger.error(e)

    
    if len(lista_cont) == 1:
        logger.debug('Ci sono contenitori nella piazzola {0}'.format(id_piazzola))
        for cc in lista_cont:
            return cc[1], cc[2]
    else:
        logger.debug('Non ci sono più contenitori nella piazzola {0}'.format(id_piazzola))
        return 'nd', 'nd'
    curr.close()



def main():
    #################################################################
    logger.info('Connessione al db SIT')
    conn = psycopg2.connect(dbname=db,
                        port=port,
                        user=user,
                        password=pwd,
                        host=host)
    
    
    
    # cerco le piazzole bilaterali censite sul SIT
    
    query_sit='''select id_elemento, te.tipo_elemento, tr.nome, tr.codice_cer, te.volume, id_piazzola, matricola, tag
    from elem.elementi e 
    join elem.tipi_elemento te on te.tipo_elemento = e.tipo_elemento 
    join elem.tipi_rifiuto tr on tr.tipo_rifiuto = te.tipo_rifiuto 
    where e.tipo_elemento in (
    select tipo_elemento  from elem.tipi_elemento te where descrizione ilike '%bilat%'
    ) and id_piazzola in (select id_piazzola from elem.piazzole p where data_eliminazione is null)
    order by id_piazzola, tr.codice_cer, id_elemento''' 
    
    curr = conn.cursor()
    try:
        #curr.execute(query_sit, (id_piazzola,))
        curr.execute(query_sit)
        lista_elementi_sit=curr.fetchall()
    except Exception as e:
        logger.error(e)
    
    
    pdr_numero_errato=[]
    pdr_idea_mancante=[]
    tipo_numero_errato=[]
    tipo_idea_mancante=[]
    
    
    for el_sit in lista_elementi_sit:
        # cerco quanti sono su SIT
        conto_sit='''select id_elemento, matricola, tag
        from elem.elementi e 
        join elem.tipi_elemento te on te.tipo_elemento = e.tipo_elemento 
        where e.id_piazzola = %s and e.tipo_elemento = %s and te.volume = %s'''
        curr1 = conn.cursor()
        try:
            #curr.execute(query_sit, (id_piazzola,))
            curr1.execute(conto_sit, (el_sit[5], el_sit[1], el_sit[4]))
            el_sit_tipo=curr1.fetchall()
        except Exception as e:
            logger.error(e)

    
    
        curr1.close()
        
        
        
        
        # cerco quanti sono su IDEA
        
        elementi_idea= '''select ci.id_piazzola, ci.id_elemento_idea , ci.volume_contenitore, ci.targa_contenitore,
        ci.tag_contenitore, ci.sim_numtel, ci.id_elettronica
        from idea.censimento_idea ci 
        join idea.codici_cer cc on cc.codice_cer =ci.cod_cer_mat 
        where id_piazzola = %s::text
        and cc.codice_cer_corretto = %s 
        and ci.volume_contenitore = %s
        order by id_elemento_idea '''
        
        curr1 = conn.cursor()
        try:
            #curr.execute(query_sit, (id_piazzola,))
            curr1.execute(elementi_idea, (el_sit[5], el_sit[3], el_sit[4]))
            el_idea_tipo=curr1.fetchall()
        except Exception as e:
            logger.error(e)
            
            
        if len(el_sit_tipo) == len(el_idea_tipo):
            curr2 = conn.cursor()
            i=0
            while i<len(el_idea_tipo):
                query_update='''UPDATE elem.elementi
                                SET matricola=%s, tag=%s 
                                WHERE id_elemento=%s::numeric'''
                try:
                    #curr.execute(query_sit, (id_piazzola,))
                    curr2.execute(query_update, (el_idea_tipo[i][3], el_idea_tipo[i][4], el_sit_tipo[i][0]))
                except Exception as e:
                    logger.error(query_update, (el_idea_tipo[i][3], el_idea_tipo[i][4], el_sit_tipo[i][0]))
                    logger.error(e)
                i+=1
            #conn.commit()
            curr2.close()
            
        elif len(el_idea_tipo)==0:
            pdr_idea_mancante.append(el_sit[5])
            tipo_idea_mancante.append(el_sit[1])
            logger.info('Non risultano elementi di tipo {} nella piazzola {}. Su SIT ne risultano {} '.format(el_sit[1], el_sit[5], len(el_sit_tipo)))

        else:
            pdr_numero_errato.append(el_sit[5])
            tipo_numero_errato.append(el_sit[1])
            logger.info('Il numero di elementi di tipo {} della piazzola {} è errato. SIT: {} IDEA:{}'.format(el_sit[1], el_sit[5], len(el_sit_tipo), len(el_idea_tipo)))
        
        curr1.close()
        
    curr.close()
    
    
    curr = conn.cursor()
    pdr_idea_mancante_ok=distinct_list(pdr_idea_mancante)
    pdr_numero_errato_ok=distinct_list(pdr_numero_errato)
    
    if len(pdr_idea_mancante_ok)>0 or len(pdr_numero_errato_ok)>0 :
        file_piazzole2="{0}/{1}_anomalie_piazzole.xlsx".format(path, giorno_file)
        workbook = xlsxwriter.Workbook(file_piazzole2)
    
        # SHEET PIAZZOLE MANCANTI
        if len(pdr_idea_mancante_ok)>0:
            w1 = workbook.add_worksheet('Piazzole mancanti')

            w1.set_tab_color('red')
            #date_format = workbook.add_format({'num_format': 'dd/mm/yyyy hh:mm'})

            title = workbook.add_format({'bold': True, 'bg_color': '#F9FF33', 'valign': 'vcenter', 'center_across': True,'text_wrap': True})
            text = workbook.add_format({'text_wrap': True})
            #text_green = workbook.add_format({'text_wrap': True, 'bg_color': '#ccffee'})
            #date_format = workbook.add_format({'num_format': 'dd/mm/yyyy hh:mm', 'bg_color': '#ccffee'})
            #text_red= workbook.add_format({'text_wrap': True, 'bg_color': '#ffcc99'})
            #date_format_dispari = workbook.add_format({'num_format': 'dd/mm/yyyy hh:mm', 'bg_color': '#ffcc99'})

            w1.set_column(0, 2, 15)
            w1.set_column(3, 4, 60)
            #w1.set_column(3, 3, 30)
            #w1.set_column(4, 4, 10)
            #w1.set_column(5, 5, 40)


            w1.write(0, 0, 'ID_PIAZZOLA', title) 
            w1.write(0, 1, 'MUNICIPIO', title) 
            w1.write(0, 2, 'QUARTIERE', title) 
            w1.write(0, 3, 'DESCRIZIONE', title)
            w1.write(0, 4, 'ELEMENTI', title)

            i=0
            while i < len(pdr_idea_mancante_ok):
                query_select='''select id_piazzola, municipio, quartiere, 
                concat(via, ' ,', civ, ' - ', riferimento) as descrizione, elem 
                from elem.v_piazzole_dwh vpd where id_piazzola = %s '''
                try:
                    #curr.execute(query_sit, (id_piazzola,))
                    curr.execute(query_select, (pdr_idea_mancante_ok[i],))
                    piazzole=curr.fetchall()
                except Exception as e:
                    logger.error(e)
                
                for p in piazzole:
                    cc=0
                    while cc < len(p):
                        w1.write(i+1, cc, p[cc])
                        cc+=1
                i+=1

        if len(pdr_numero_errato_ok)>0:
            w1 = workbook.add_worksheet('Piazzole con elementi non corrispondenti')

            w1.set_tab_color('yellow')
            #date_format = workbook.add_format({'num_format': 'dd/mm/yyyy hh:mm'})

            title = workbook.add_format({'bold': True, 'bg_color': '#F9FF33', 'valign': 'vcenter', 'center_across': True,'text_wrap': True})
            text = workbook.add_format({'text_wrap': True})
            #text_green = workbook.add_format({'text_wrap': True, 'bg_color': '#ccffee'})
            #date_format = workbook.add_format({'num_format': 'dd/mm/yyyy hh:mm', 'bg_color': '#ccffee'})
            #text_red= workbook.add_format({'text_wrap': True, 'bg_color': '#ffcc99'})
            #date_format_dispari = workbook.add_format({'num_format': 'dd/mm/yyyy hh:mm', 'bg_color': '#ffcc99'})

            w1.set_column(0, 2, 15)
            w1.set_column(3, 4, 60)
            #w1.set_column(3, 3, 30)
            #w1.set_column(4, 4, 10)
            #w1.set_column(5, 5, 40)


            w1.write(0, 0, 'ID_PIAZZOLA', title) 
            w1.write(0, 1, 'MUNICIPIO', title) 
            w1.write(0, 2, 'QUARTIERE', title) 
            w1.write(0, 3, 'DESCRIZIONE_SIT', title)
            w1.write(0, 4, 'DESCRIZIONE_IDEA', title)
            w1.write(0, 3, 'ELEMENTI_SIT', title)
            w1.write(0, 4, 'ELEMENTI_IDEA', title)

            i=0
            while i < len(pdr_idea_mancante_ok):
                query_select='''select vpd.id_piazzola, municipio, quartiere,
                concat(via, ' ,', civ, ' - ', riferimento) as descrizione_sit,
                vpi.indirizzo_idea, 
                vpi.elementi_idea,
                vpi.elementi_sit 
                from idea.v_piazzole_idea vpi 
                join elem.v_piazzole_dwh vpd on vpd.id_piazzola::text = vpi.id_piazzola 
                where vpd.id_piazzola =  %s'''
                try:
                    #curr.execute(query_sit, (id_piazzola,))
                    curr.execute(query_select, (pdr_idea_mancante_ok[i],))
                    piazzole=curr.fetchall()
                except Exception as e:
                    logger.error(e)
                
                for p in piazzole:
                    cc=0
                    while cc < len(p):
                        w1.write(i+1, cc, p[cc])
                        cc+=1
                i+=1
        workbook.close()
        
        ################################
        # predisposizione mail
        ################################

        # Create a secure SSL context
        context = ssl.create_default_context()

        subject = "ANOMALIE PIAZZOLE"
        body = '''Mail generata automaticamente dal codice python pdr_albero_rimozione.py che gira su server amiugis interrogando i WS di ID&A con il censimento delle piazzole.\n\n
        \n\n\n\n
        AMIU Assistenza Territorio
        '''
        #sender_email = user_mail
        receiver_email='assterritorio@amiu.genova.it'
        debug_email='roberto.marzocchi@amiu.genova.it'
        #cc_mail='pianar@amiu.genova.it'

        # Create a multipart message and set headers
        message = MIMEMultipart()
        message["From"] = sender_email
        message["To"] = debug_email
        #message["Cc"] = cc_mail
        message["Subject"] = subject
        #message["Bcc"] = debug_email  # Recommended for mass emails
        message.preamble = "Contenitori rimossi"

        
                        
        # Add body to email
        message.attach(MIMEText(body, "plain"))

        # aggiunto allegato (usando la funzione importata)
        allegato(message, file_piazzole2, '{}_contenitori_rimossi.xlsx'.format(giorno_file))
        
        #text = message.as_string()

        # Now send or store the message
        logging.info("Richiamo la funzione per inviare mail")
        invio=invio_messaggio(message)
        logging.info(invio)
    else:
        logger.info('Non ci sono elementi da rimuovere')



    # check se c_handller contiene almeno una riga 
    error_log_mail(errorfile, 'roberto.marzocchi@amiu.genova.it', os.path.basename(__file__), logger)

    logger.info("Chiudo definitivamente la connesione al DB")
    
    
    
    curr.close()
    conn.close()
    
    
    
    
if __name__ == "__main__":
    main()   