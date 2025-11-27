#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2021
# Roberto Marzocchi

'''
Lo script 

1) verifica se i marker attesi ci sono
2) se non ci fossero li crea con il colore giusto


Fa un check delle foto presenti nella cartella foto e aggiorna la tabella elem.piazzole di conseguenza per tenere conto delle foto aggiunte da altre fonti

'''

import os, sys, re  ,shutil #,glob
import inspect, os.path

import xlsxwriter


#import getopt  # per gestire gli input

#import pymssql

import psycopg2

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

# per gestire le immagini
from PIL import Image
import sys
import csv

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
logfile='{}/log/crea_marker.log'.format(path)
errorfile='{}/log/error_crea_marker.log'.format(path)
#if os.path.exists(logfile):
#    os.remove(logfile)

import fnmatch # per filtrare i tipi file





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




def hex_to_rgb(hex):
  return tuple(int(hex[i:i+2], 16) for i in (0, 2, 4))


def hex_to_rgba(hex):
  return tuple(int(hex[i:i+2], 16) for i in (0, 2, 4, 6))

def main():
    # carico i mezzi sul DB PostgreSQL
    logger.info('Connessione al db {}'.format(db))
    conn = psycopg2.connect(dbname=db,
                        port=port,
                        user=user,
                        password=pwd,
                        host=host)

    curr = conn.cursor()
    
    logger.info('Connessione al db {}'.format(db_test))
    connt = psycopg2.connect(dbname=db_test,
                        port=port,
                        user=user,
                        password=pwd,
                        host=host_test)

    currt = connt.cursor()
    #conn.autocommit = True
    
    
    query='''select tipo_rifiuto, colore
from elem.tipi_rifiuto where tipo_rifiuto > 1 order by tipo_rifiuto'''


    try:
        curr.execute(query)
        lista_rifiuti=curr.fetchall()
    except Exception as e:
        logger.error(e)



    #lista_file=['ecopunto_g','ecopunto_p','ecopunto_p_g','ecopunto_p_p', 'grande', 'piccola', 'privata_g', 'privata_p']
    lista_file=['grande', 'piccola', 'privata_g', 'privata_p']        
    for rr in lista_rifiuti:
        logger.debug(rr[0])
        k=0
        while k< len(lista_file):
            file_originale= "{0}/img_sit/markers/0-{1}.png".format(path, lista_file[k])
            filename_creato="{0}/img_sit/markers/{1}-{2}.png".format(path, rr[0], lista_file[k])
            
            #logger.debug(filename)
            #logger.info(os.path.exists(filename))
            # a questo punto verifico
            if (os.path.exists(filename)):
                logger.debug('''Il file c'è non devo fare nulla''')
            else: # il file non esiste
                logger.debug('''Devo creare file {0} per rifiuto {1} di colore {2}'''.format(filename, rr[0], rr[1]))
                
                
                shutil.copy(file_originale, filename_creato)
                picture = Image.open(filename_creato)

                picture = picture.convert("RGBA")
                pixdata = picture.load()
                # Get the size of the image
                width, height = picture.size
                COLOR_TO_DELETE = hex_to_rgb('ff00bb')
                FUZZINESS = 15
                color_rgb=hex_to_rgb(rr[1].replace('#', ''))
                if lista_file[k]=='piccola' or lista_file[k]=='privata_p':
                    color_rgb=hex_to_rgba('{}80'.format(rr[1].replace('#', '')))
                else:
                    color_rgb=hex_to_rgb('{}'.format(rr[1].replace('#', '')))
                    
                    
                    
                    
            
                # Process every pixel
                for y in range(picture.size[1]):
                    for x in range(picture.size[0]):
                        
                        
                        
                        totalColorDiff = 0
                        shouldReplace = True
                        for idx in range(3): #include RGB but exclude alpha
                            if abs(pixdata[x, y][idx] - COLOR_TO_DELETE[idx]) > FUZZINESS:
                                shouldReplace = False
                        
                        if shouldReplace:
                            #print('Sostituisco colore {}'.format(color_rgb))
                            pixdata[x, y] = color_rgb #replace
                            
                        '''
                        current_color = picture.getpixel( (x,y) )
                        ####################################################################
                        # Do your logic here and create a new (R,G,B) tuple called new_color
                        ####################################################################
                        #logger.debug(current_color)
                        color_rgb=hex_to_rgb(rr[1].replace('#', ''))
                        #logger.debug(color_rgb)
                        picture.putpixel( (x,y), color_rgb)
                        '''
                picture.save(filename_creato)
                picture.close
                #exit()
                
                
            k+=1
    curr.close()
    curr = conn.cursor()
      
    # Ora faccio il check delle foto presenti su SIT e aggiorno il DB
    files = fnmatch.filter(os.listdir('{}/img_sit/Foto/'.format(path)),'*.jpg')
        
    k=0
    while k< len(files):
        update_foto='''UPDATE elem.piazzole SET foto = 1 WHERE id_piazzola = %s and foto = 0'''
        try:
            curr.execute(update_foto, (files[k].split('.')[0],))
            currt.execute(update_foto, (files[k].split('.')[0],))
        except Exception as e:
            logger.error(e)
        
        
        conn.commit()
        connt.commit()
        k+=1
    
    
     
    curr.close()
    conn.close()
    currt.close()
    connt.close()
       
       
    # check se c_handller contiene almeno una riga 
    error_log_mail(errorfile, 'assterritorio@amiu.genova.it', os.path.basename(__file__), logger)
    
    
    
     
if __name__ == "__main__":
    main()   