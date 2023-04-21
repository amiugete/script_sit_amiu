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
path1 = os.path.dirname(os.path.dirname(os.path.abspath(filename)))
giorno_file=datetime.datetime.today().strftime('%Y%m%d%H%M')



#tmpfolder=tempfile.gettempdir() # get the current temporary directory
logfile='{}/pdrAlbero_rimozione.log'.format(path)
errorfile='{}/error_pdrAlbero_rimozione.log'.format(path)
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
    headers1 = {''' {0}'''.format(token1)}
    
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
    #nuovi_cont=[]
    #nuove_targhe=[]

    old_id_pdr=[]
    old_id_cont=[]
    old_id_targa=[]

    while check<1:
        logger.info('Page index {}'.format(p))
        # se volessi usare un singolo id_pdr per debug (ricordarsi di modificare anche il logger)
        #response = requests.get(api_url, params={'id_pdr':'23015', 'page_size': 100, 'page_index': p}, headers={'Authorization': 'Token {}'.format(token1)})
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
            logger.debug(letture['data'])
            
            logger.debug('Lette {} righe dalle API'.format(len(letture['data'])))
            
            
            #exit()
            
            if len(letture['data'])>=3:
                logger.debug('****************************************************')
                logger.debug(letture['data'][1][0])
                logger.debug('****************************************************')
            # se non ci sono più dati annullo
            if len(letture['data'])==0:
                check=100
            
            logger.debug(check)

            i=0
            while i < len(letture['data']):
                logger.debug('Sono qua')
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
                            
                            # se non ha elettronica vuol dire che è stato spostato
                            try: 
                            
                                k=0
                                while k < len(letture['data'][i][0]['contenitori'][j]['elettroniche']):
                                    cod_elettronica=letture['data'][i][0]['contenitori'][j]['elettroniche'][k]['cod_elett']
                                    desc_elettronica=letture['data'][i][0]['contenitori'][j]['elettroniche'][k]['cod_elett']
                                    val_bat_e=letture['data'][i][0]['contenitori'][j]['elettroniche'][k]['val_bat']
                                    iccid=letture['data'][i][0]['contenitori'][j]['elettroniche'][k]['iccid']
                                    if iccid ==None: 
                                        logger.warning('iccid ND')
                                        iccid='ND'
                                    
                                    num_tel=letture['data'][i][0]['contenitori'][j]['elettroniche'][k]['num_tel']
                                    #num_tel=num_tel.strip()
                                    
                                    if num_tel == None: 
                                        logger.warning('num_tel ND')
                                        num_tel='ND'
                                    else:
                                        num_tel=num_tel.strip()
                                        
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
                                        query_select="SELECT id_bocchetta FROM idea.censimento_idea WHERE id_elemento_idea=%s"
                                        try:
                                            curr.execute(query_select, (id_cont,))
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
                                            data_ultimo_agg=%s, val_riemp=%s, val_bat_bocchetta=%s, geoloc=st_transform(ST_SetSRID(ST_MakePoint(%s, %s),4326),3003),
                                            data_agg_api=now()
                                            WHERE id_elemento_idea=%s;'''
                                            try:
                                                curr.execute(query_update, (id_pdr,descrizione_pdr,id_cont,tipo_cont,vol_cont,targa_cont,tag_cont,cod_elettronica,
                                                desc_elettronica,iccid,num_tel,val_bat_e,id_bocc,cod_elett_sens,cod_cer_mat,volume_b,data_ultimo_agg,
                                                val_riemp,val_bat_b,lon,lat,id_cont))
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
                            except:
                                logger.warning('Il contenitore {0} non ha elettronica, lo considero rimosso'.format(id_cont))
                                # controllo se ho il contenitore sul DB
                                query_select = '''SELECT id_piazzola FROM idea.censimento_idea WHERE id_elemento_idea=%s'''
                                curr.execute(query_select, (id_cont,))
                                lista_pp=curr.fetchall()
                                if len(lista_pp)>0:
                                    query_delete='''DELETE FROM idea.censimento_idea WHERE id_elemento_idea=%s'''
                                    curr.execute(query_delete, (id_cont,))
                                    old_id_pdr.append(id_pdr)
                                    old_id_cont.append(id_cont)
                                    old_id_targa.append(targa_cont)
                                else:
                                    logger.debug('Non faccio nulla')
                            logger.info('Contenitore: {0}'.format(id_cont))
                            j+=1


                        
                        ########################################################################################
                        # da testare sempre prima senza fare i commit per verificare che sia tutto OK
                        conn.commit()
                        ########################################################################################
                except Exception as e:
                    logger.error("Postazione {0} - Errore {1}".format(letture['data'][i][0]['id_pdr'],e))    
                
                #print(i,letture['data'][i][9], letture['data'][i][10], letture['data'][i][14], letture['data'][i][16],letture['data'][i][17])
                i+=1
            p+=1
   
    
    curr.close()
    #curr = conn.cursor()
    #conn.close()
    logger.info('Ora verifico se ci sono da scrivere mail - {} - {}'.format(len(nuovi_id), len(old_id_pdr)))
    


    nuovi_id_ok=distinct_list(nuovi_id)
    nuove_desc_ok=distinct_list(nuove_desc)
    if len(nuovi_id_ok)>0:
        logger.info('Ci sono nuove piazzole - Predispongo il file e mando mail')
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
        w1.set_column(1, 4, 60)
        #w1.set_column(3, 3, 30)
        #w1.set_column(4, 4, 10)
        #w1.set_column(5, 5, 40)


        w1.write(0, 0, 'ID_PIAZZOLA', title) 
        w1.write(0, 1, 'DESCRIZIONE_IDEA', title) 
        w1.write(0, 2, 'ID_CONTENITORI_IDEA', title) 
        w1.write(0, 3, 'TARGHE CONTENITORI', title) 

        i=0 
        while i<len(nuovi_id_ok):
            i+=1
            w1.write(i,0, nuovi_id_ok[i-1],text)
            w1.write(i,1, nuove_desc_ok[i-1],text)
            w1.write(i,2, contenitori_piazzola(conn, logger, nuovi_id_ok[i-1])[0], text)
            w1.write(i,3, contenitori_piazzola(conn, logger, nuovi_id_ok[i-1])[1], text)
        workbook.close()
        
        ################################
        # predisposizione mail
        ################################

        # Create a secure SSL context
        context = ssl.create_default_context()

        subject = "NUOVE PIAZZOLE BILATERALI CENSITE DA ID&A"
        body = '''Mail generata automaticamente dal codice python pdr_albero_rimozione.py che gira su server amiugis interrogando i WS di ID&A con il censimento delle piazzole.\n\n
        <br><br><br>
        AMIU Assistenza Territorio<br>
        <img src="cid:image1" alt="Logo" width=197>
        <br>
        '''
        #sender_email = user_mail
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
        message.attach(MIMEText(body, "html"))
        
        #aggiungo logo 
        logoname='{}/img/logo_amiu.jpg'.format(path1)
        immagine(message,logoname)

        # aggiunto allegato (usando la funzione importata)
        allegato(message, file_piazzole, '{}_piazzole_nuove_aggiornate.xlsx'.format(giorno_file))
        
        #text = message.as_string()

        # Now send or store the message
        logging.info("Richiamo la funzione per inviare mail")
        invio=invio_messaggio(message)
        logging.info(invio)

    else:
        logger.info('Non ci sono nuovi elementi')

    
    old_id_pdr_ok=distinct_list(old_id_pdr)
    logger.debug('Sono qua - {}'.format(len(old_id_pdr_ok)))
    if len(old_id_pdr_ok)>0:

        # Imposto file con contenitori rimossi
        file_piazzole2="{0}/{1}_contenitori_rimossi.xlsx".format(path, giorno_file)
        workbook = xlsxwriter.Workbook(file_piazzole2)
        w1 = workbook.add_worksheet('Elementi rimossi')

        w1.set_tab_color('red')
        #date_format = workbook.add_format({'num_format': 'dd/mm/yyyy hh:mm'})

        title = workbook.add_format({'bold': True, 'bg_color': '#F9FF33', 'valign': 'vcenter', 'center_across': True,'text_wrap': True})
        text = workbook.add_format({'text_wrap': True})
        text_green = workbook.add_format({'text_wrap': True, 'bg_color': '#ccffee'})
        #date_format = workbook.add_format({'num_format': 'dd/mm/yyyy hh:mm', 'bg_color': '#ccffee'})
        text_red= workbook.add_format({'text_wrap': True, 'bg_color': '#ffcc99'})
        #date_format_dispari = workbook.add_format({'num_format': 'dd/mm/yyyy hh:mm', 'bg_color': '#ffcc99'})

        w1.set_column(0, 0, 15)
        w1.set_column(1, 2, 60)
        w1.set_column(3, 3, 35)
        #w1.set_column(3, 3, 30)
        #w1.set_column(4, 4, 10)
        #w1.set_column(5, 5, 40)


        w1.write(0, 0, 'ID_PIAZZOLA', title) 
        w1.write(0, 1, 'ID_CONTENITORI_IDEA', title) 
        w1.write(0, 2, 'TARGHE CONTENITORI', title) 
        w1.write(0, 3, 'STATO', title)

        i=0 
        r=1
        logger.debug('i={}, r={}'.format(i,r))
        while i<len(old_id_pdr_ok):
            w1.write(r,0, old_id_pdr_ok[i],text_red)
            cont_rimossi=''
            targhe_rimosse=''
            kk=0
            logger.debug('i={}, r={}, kk={}'.format(i,r,kk))
            while kk<len(old_id_cont):
                if old_id_pdr[kk]==old_id_pdr_ok[i]:
                    if cont_rimossi=='':
                        cont_rimossi= '{0}'.format(old_id_cont[kk])
                        targhe_rimosse= '{0}'.format(old_id_targa[kk])
                    else:
                        cont_rimossi='{0}, {1}'.format(cont_rimossi,old_id_cont[kk])
                        targhe_rimosse='{0}, {1}'.format(targhe_rimosse,old_id_targa[kk])
                    kk+=1
                else:
                    kk+=1
            w1.write(r,1, cont_rimossi, text_red)
            w1.write(r,2, targhe_rimosse, text_red)
            w1.write(r,3, 'rimossi',text_red)
            r+=1
            # ora verifico se sono rimasti dei contenitori
            if contenitori_piazzola(conn, logger, old_id_pdr_ok[i])[0]!='nd':
                w1.write(r,0, old_id_pdr_ok[i],text_green)
                w1.write(r,1, contenitori_piazzola(conn, logger, old_id_pdr_ok[i])[0], text_green)
                w1.write(r,2, contenitori_piazzola(conn, logger, old_id_pdr_ok[i])[1], text_green)
                w1.write(r,3, 'mantenuti', text_green)
                r+=1
            i+=1

        logger.debug('i={}, r={}, kk={}'.format(i,r,kk))

        workbook.close()
        
        ################################
        # predisposizione mail
        ################################

        # Create a secure SSL context
        context = ssl.create_default_context()

        subject = "CONTENITORI RIMOSSI / CON PROBLEMI ELETTRONICA (DA ID&A)"
        body = '''Mail generata automaticamente dal codice python pdr_albero_rimozione.py che gira su server amiugis interrogando i WS di ID&A con il censimento delle piazzole.\n\n
        <br><br><br>
        AMIU Assistenza Territorio<br>
        <img src="cid:image1" alt="Logo" width=197>
        <br>
        '''
        #sender_email = user_mail
        receiver_email='assterritorio@amiu.genova.it'
        debug_email='roberto.marzocchi@amiu.genova.it'
        cc_mail='marco.zamboni@ideabs.com; valentina.anamiti@amiu.genova.it'

        # Create a multipart message and set headers
        message = MIMEMultipart()
        message["From"] = sender_email
        message["To"] = receiver_email
        message["Cc"] = cc_mail
        message["Subject"] = subject
        #message["Bcc"] = debug_email  # Recommended for mass emails
        message.preamble = "Contenitori rimossi"

        
                        
        # Add body to email
        message.attach(MIMEText(body, "html"))
        
        #aggiungo logo 
        logoname='{}/img/logo_amiu.jpg'.format(path1)
        immagine(message,logoname)

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
    #curr.close()
    conn.close()

    #while i
    
    
    
    
if __name__ == "__main__":
    main()   