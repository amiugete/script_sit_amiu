#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2023
# Roberto Marzocchi

'''
calcolo fascia turno
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



# per mandare file a EKOVISION
import pysftp


import requests
from requests.exceptions import HTTPError

import json

import logging

path=os.path.dirname(sys.argv[0]) 
#tmpfolder=tempfile.gettempdir() # get the current temporary directory
logfile='{}/log/test_fascia_turno.log'.format(path)
errorfile='{}/log/error_test_fascia_turno.log'.format(path)
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

# libreria per scrivere file csv
import csv



def fascia_turno(ora_inizio_lav, ora_fine_lav, ora_inizio_lav_2 ,ora_fine_lav_2):
    '''
    Calcolo della fascia turno sulla base degli orari della scheda di lavoro Ekovision
    '''
    fascia_turno=''
    if ora_inizio_lav_2 == '000000' and ora_fine_lav_2 =='000000':
    
        if ora_inizio_lav== '000000' and ora_fine_lav =='000000':
            fascia_turno='D'
        else:
            oi=int(ora_inizio_lav[:2])
            mi=int(ora_inizio_lav[2:4])
            of=int(ora_fine_lav[:2])
            mf=int(ora_fine_lav[2:4])
    else:
        oi=int(ora_inizio_lav[:2])
        mi=int(ora_inizio_lav[2:4])
        of=int(ora_fine_lav_2[:2])
        mf=int(ora_fine_lav_2[2:4])
            
            
    if fascia_turno=='':        
        # calcolo minuti del turno
        if of < oi:
            minuti= 60*(24 - oi) + 60 * of - mi + mf
        else :
            minuti = 60 * (of-oi) - mi + mf 

        
        hh_plus=int(minuti/2/60)
        mm_plus=minuti/2-60*int(minuti/2/60)
        
        # ora media
        if mi+mm_plus >= 60:
            mm=mi+mm_plus-60
            hh=oi+1+hh_plus
        else:
            mm=mi+mm_plus
            hh=oi+hh_plus
        
        print('{}:{}'.format(hh,mm))
        
        if hh > 5 and hh <= 12:
            fascia_turno = 'M'
        elif hh > 12 and hh <= 20:
            fascia_turno = 'P'
        elif hh > 20 or hh <= 5:
            fascia_turno= 'N'
        
        return fascia_turno

        
        
                
        
     

def main():
      

    ora_inizio_lav = '030000'
    ora_fine_lav = '120000'
    ora_inizio_lav_2 = '000000'
    ora_fine_lav_2 = '000000'

    f= fascia_turno(ora_inizio_lav, ora_fine_lav, ora_inizio_lav_2 ,ora_fine_lav_2)

    logger.info(f)



if __name__ == "__main__":
    main()      