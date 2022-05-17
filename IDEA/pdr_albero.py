#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2021
# Roberto Marzocchi

'''
Lo script interroga i WS di IDEA

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
logfile='{}/pdrAlbero.log'.format(path)
errorfile='{}/error_pdrAlbero.log'.format(path)
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
#f_handler = logging.StreamHandler()
f_handler = logging.FileHandler(filename=logfile, encoding='utf-8', mode='w')


c_handler.setLevel(logging.ERROR)
f_handler.setLevel(logging.INFO)


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
from allegato_mail import *



def distinct_list(seq): # Order preserving
  ''' Modified version of Dave Kirby solution '''
  seen = set()
  return [x for x in seq if x not in seen and not seen.add(x)]


def main():
    #################################################################
    logger.info("Recupero il token")
    token1=token()
    print(token1)
    logger.debug(token1)
    #################################################################
    logger.info('Connessione al db SIT')
    conn = psycopg2.connect(dbname=db,
                        port=port,
                        user=user,
                        password=pwd,
                        host=host)

    curr = conn.cursor()
    #conn.autocommit = True
    #################################################################
    api_url='{}/pdralbero'.format(url_idea)
    headers1 = {'''Authorization: Token {0}'''.format(token1)}
    
    # per ora re-importo tutto, poi sarà da sistematre 
    '''query_truncate="TRUNCATE TABLE idea.conferimenti_horus CONTINUE IDENTITY RESTRICT;"
    try:
        curr.execute(query_truncate)
    except Exception as e:
        logger.error(e)
    ########################################################################################
    # da testare sempre prima senza fare i commit per verificare che sia tutto OK
    conn.commit()
    ########################################################################################
    '''
    curr.close()
    curr = conn.cursor()
    #print(headers1)
    #exit()
    p=1
    check=0
    
    

    
    nuovi_id=[]
    nuove_desc=[]

    while check<1:
        logger.info('Page index {}'.format(p))
        response = requests.get(api_url, params={'page_size': 100, 'page_index': p}, headers={'Authorization': 'Token {}'.format(token1)})
        #response.json()
        logger.debug(response.status_code)
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
            
            colonne=letture['meta']['columns']
            
            logger.debug(len(colonne))
            logger.debug(colonne)
            
            
            logger.debug('Lette {} righe dalle API'.format(len(letture['data'])))
            
            
            #exit()
            
            if len(letture['data'])>=3:
                logger.debug('****************************************************')
                logger.debug(letture['data'][1][0])
                logger.debug('****************************************************')
            # se non ci sono più dati annullo
            if len(letture['data'])==0:
                check=100
            
            i=0
            while i < len(letture['data']):
                # 16 lat 17 long
                # 9 codice elemento IDEA
                # 14 data 
                # 6 codice badg 
                # 7 id_user
                logger.debug('i={}'.format(i))
                logger.info(letture['data'][i][0]['id_pdr'])
                #exit()
                try:
                    if float(letture['data'][i][0]['lat'])>0:
                        #id_isola
                        id_pdr=letture['data'][i][0]['id_pdr']
                        #id_comune=letture['data'][i][0]['id_comune']
                        descrizione_pdr=letture['data'][i][0]['desc_pdr']
                        lat=float(letture['data'][i][0]['lat'])
                        lon=float(letture['data'][i][0]['lng'])
                        logger.debug('lat={}'.format(lat))
                        j=0
                        while j < len(letture['data'][i][0]['contenitori']):
                            logger.debug('i={} / j={}'.format(i,j))
                            id_cont=letture['data'][i][0]['contenitori'][j]['id_cont']
                            targa_cont=letture['data'][i][0]['contenitori'][j]['targa']
                            #desc_cont=letture['data'][i][0]['contenitori'][j]['id_cont']
                            tipo_cont=letture['data'][i][0]['contenitori'][j]['tipo_contenitore']
                            vol_cont=letture['data'][i][0]['contenitori'][j]['volume']
                            tag_cont=letture['data'][i][0]['contenitori'][j]['tag']
                            k=0
                            while k < len(letture['data'][i][0]['contenitori'][j]['elettroniche']):
                                cod_elettronica=letture['data'][i][0]['contenitori'][j]['elettroniche'][k]['cod_elett']
                                desc_elettronica=letture['data'][i][0]['contenitori'][j]['elettroniche'][k]['cod_elett']
                                val_bat_e=letture['data'][i][0]['contenitori'][j]['elettroniche'][k]['val_bat']
                                iccid=letture['data'][i][0]['contenitori'][j]['elettroniche'][k]['iccid']
                                num_tel=letture['data'][i][0]['contenitori'][j]['elettroniche'][k]['cod_elett'].strip()
                                f=0
                                while f < len(letture['data'][i][0]['contenitori'][j]['elettroniche'][k]['bocchette']):
                                    id_bocc=letture['data'][i][0]['contenitori'][j]['elettroniche'][k]['bocchette'][f]['id_bocc']
                                    cod_elett_sens=letture['data'][i][0]['contenitori'][j]['elettroniche'][k]['bocchette'][f]['cod_elett_sens']
                                    data_ultimo_agg=datetime.datetime.strptime(letture['data'][i][0]['contenitori'][j]['elettroniche'][k]['bocchette'][f]['data_ultimo_agg'], "%Y%m%d%H%M%S").strftime("%Y/%m/%d %H:%M:%S")
                                    cod_cer_mat=letture['data'][i][0]['contenitori'][j]['elettroniche'][k]['bocchette'][f]['cod_cer_mat']
                                    #desc_mat=letture['data'][i][0]['contenitori'][j]['elettroniche'][k]['bocchette'][f]['desc_mat']
                                    val_riemp=letture['data'][i][0]['contenitori'][j]['elettroniche'][k]['bocchette'][f]['val_riemp']
                                    val_bat_b=letture['data'][i][0]['contenitori'][j]['elettroniche'][k]['bocchette'][f]['val_riemp']
                                    volume_b=letture['data'][i][0]['contenitori'][j]['elettroniche'][k]['bocchette'][f]['volume']
                                    f+=1
                                    query_select="SELECT id_bocchetta FROM idea.censimento_idea WHERE id_bocchetta=%s"
                                    try:
                                        curr.execute(query_select, (id_bocc,))
                                        bocchetta=curr.fetchall()
                                    except Exception as e:
                                        logger.error(e)
                                    curr.close()
                                    curr = conn.cursor()
                                    # se c'è già la entry faccio 
                                    if len(bocchetta)>0:
                                        query_update='''UPDATE idea.censimento_idea
                                        SET id_piazzola=%s,  indirizzo_idea=%s, id_elemento_idea=%s, tipo_contenitore=%s,
                                        volume_contenitore=%s, targa_contenitore=%s, tag_contenitore=%s,
                                        id_elettronica=%s, desc_elett=%s, iccidsim=%s, sim_numtel=%s, val_bat_elettronica=%s,
                                        id_bocchetta=%s, cod_elett_sens=%s, cod_cer_mat=%s, volume_bocchetta=%s,
                                        data_ultimo_agg=%s, val_riemp=%s, val_bat_bocchetta=%s, geoloc=st_transform(ST_SetSRID(ST_MakePoint(%s, %s),4326),3003) 
                                        WHERE id_bocchetta=%s;'''
                                        try:
                                            curr.execute(query_update, (id_pdr,descrizione_pdr,id_cont,tipo_cont,vol_cont,targa_cont,tag_cont,cod_elettronica,
                                            desc_elettronica,iccid,num_tel,val_bat_e,id_bocc,cod_elett_sens,cod_cer_mat,volume_b,data_ultimo_agg,
                                            val_riemp,val_bat_b,lon,lat,id_bocc))
                                        except Exception as e:
                                            logger.error(e)
                                    else:
                                        nuovi_id.append(id_pdr)
                                        nuove_desc.append(descrizione_pdr)
                                        query_insert='''INSERT INTO idea.censimento_idea
                                        (id_piazzola, indirizzo_idea, id_elemento_idea, tipo_contenitore, volume_contenitore, 
                                        targa_contenitore, tag_contenitore, id_elettronica, desc_elett, iccidsim, 
                                        sim_numtel, val_bat_elettronica, id_bocchetta, cod_elett_sens,
                                        cod_cer_mat, volume_bocchetta, data_ultimo_agg, val_riemp, val_bat_bocchetta, geoloc)
                                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,st_transform(ST_SetSRID(ST_MakePoint(%s, %s),4326),3003));
                                        '''
                                        try:
                                            curr.execute(query_insert, (id_pdr,descrizione_pdr,id_cont,tipo_cont,vol_cont,
                                            targa_cont,tag_cont,cod_elettronica,desc_elettronica,iccid,
                                            num_tel,val_bat_e,id_bocc,cod_elett_sens,
                                            cod_cer_mat,volume_b,data_ultimo_agg, val_riemp,val_bat_b,lon,lat))
                                        except Exception as e:
                                            logger.error(e)
                                k+=1
                            logger.debug(id_cont)
                            j+=1


                        
                        ########################################################################################
                        # da testare sempre prima senza fare i commit per verificare che sia tutto OK
                        conn.commit()
                        ########################################################################################
                except Exception as e:
                    logger.error(e)    
                
                #print(i,letture['data'][i][9], letture['data'][i][10], letture['data'][i][14], letture['data'][i][16],letture['data'][i][17])
                i+=1
            p+=1
   
    logger.info("Chiudo definitivamente la connesione al DB")
    curr.close()
    conn.close()
    
    
    nuovi_id_ok=distinct_list(nuovi_id)
    nuove_desc_ok=distinct_list(nuove_desc)
    if len(nuovi_id_ok)>0:
        # Imposto file con nuove piazzole
        file_piazzole="{0}/{1}_piazzole_nuove_aggiornate.xlsx".format(path, giorno_file)
        workbook = xlsxwriter.Workbook(file_piazzole)
        w1 = workbook.add_worksheet('Piazzole nuove o aggiornate')

        w1.set_tab_color('red')
        #date_format = workbook.add_format({'num_format': 'dd/mm/yyyy hh:mm'})

        title = workbook.add_format({'bold': True, 'bg_color': '#F9FF33', 'valign': 'vcenter', 'center_across': True,'text_wrap': True})
        text = workbook.add_format({'text_wrap': True})
        #text_green = workbook.add_format({'text_wrap': True, 'bg_color': '#ccffee'})
        #date_format = workbook.add_format({'num_format': 'dd/mm/yyyy hh:mm', 'bg_color': '#ccffee'})
        #text_dispari= workbook.add_format({'text_wrap': True, 'bg_color': '#ffcc99'})
        #date_format_dispari = workbook.add_format({'num_format': 'dd/mm/yyyy hh:mm', 'bg_color': '#ffcc99'})

        w1.set_column(0, 0, 15)
        w1.set_column(1, 1, 60)
        #w1.set_column(3, 3, 30)
        #w1.set_column(4, 4, 10)
        #w1.set_column(5, 5, 40)


        w1.write(0, 0, 'ID_PIAZZOLA', title) 
        w1.write(0, 1, 'DESCRIZIONE_IDEA', title) 
    
        i=0 
        while i<len(nuovi_id_ok):
            i+=1
            w1.write(i,0, nuovi_id_ok[i-1],text)
            w1.write(i,1, nuove_desc_ok[i-1],text)
        
        workbook.close()
        
        ################################
        # predisposizione mail
        ################################

        # Create a secure SSL context
        context = ssl.create_default_context()

        subject = "NUOVE PIAZZOLE BILATERALI CENSITE DA ID&A"
        body = '''Mail generata automaticamente dal codice python pdr_albero.py che gira su server amiugis interrogando i WS di ID&A con il censimento delle piazzole.\n\n
        \n\n\n\n
        AMIU Assistenza Territorio
        '''
        sender_email = user_mail
        receiver_email='assterritorio@amiu.genova.it'
        debug_email='roberto.marzocchi@amiu.genova.it'
        #cc_mail='pianar@amiu.genova.it'

        # Create a multipart message and set headers
        message = MIMEMultipart()
        message["From"] = sender_email
        message["To"] = receiver_email
        #message["Cc"] = cc_mail
        message["Subject"] = subject
        #message["Bcc"] = debug_email  # Recommended for mass emails
        message.preamble = "Nuove piazzole bilaterali"

        
                        
        # Add body to email
        message.attach(MIMEText(body, "plain"))

        # aggiunto allegato (usando la funzione importata)
        allegato(message, file_piazzole, '{}_piazzole_nuove_aggiornate.xlsx'.format(giorno_file))
        
        #text = message.as_string()

        # Now send or store the message
        with smtplib.SMTP_SSL(smtp_mail, port_mail, context=context) as s:
            s.login(user_mail, pwd_mail)
            s.send_message(message) 
    #while i
    
    
    
    
if __name__ == "__main__":
    main()   