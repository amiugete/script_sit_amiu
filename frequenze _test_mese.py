#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2021
# Roberto Marzocchi

'''
Lo script legge le frequenze del SIT espresse come numeri interi e le traduce nel formato atteso dal SW U.O. 
'''



import os, sys, re  # ,shutil,glob

#import getopt  # per gestire gli input

#import pymssql

import psycopg2

import cx_Oracle

currentdir = os.path.dirname(os.path.realpath(__file__))
parentdir = os.path.dirname(currentdir)
sys.path.append(parentdir)
from credenziali import *


#import requests

import logging

path=os.path.dirname(sys.argv[0]) 
#tmpfolder=tempfile.gettempdir() # get the current temporary directory
logfile='{}/log/frequenze.log'.format(path)
#if os.path.exists(logfile):
#    os.remove(logfile)

logging.basicConfig(
    handlers=[logging.FileHandler(filename=logfile, encoding='utf-8', mode='w')],
    format='%(asctime)s\t%(levelname)s\t%(message)s',
    #filemode='w', # overwrite or append
    #fileencoding='utf-8',
    #filename=logfile,
    level=logging.INFO)


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




def long_set(codice_binario):
    '''
        Dato un codice binario di 7 numeri dove il primo è lunedì, il secondo martedì, etc.  
            1. restituisce il long
            2. restituisce il conto dei giorni 
    '''
    array_giorni=['Lun', 'Mar', 'Mer', 'Gio', 'Ven', 'Sab', 'Dom']
    long_giorni=''
    i=0
    conto_giorni=0
    while i<7:
        if codice_binario[i:(i+1)]=='1':
            conto_giorni+=1
            if long_giorni=='':
                long_giorni='{}'.format(array_giorni[i])
            else:
                long_giorni='{}-{}'.format(long_giorni,array_giorni[i])
        i+=1
    
    return long_giorni, conto_giorni


def long_mese(codice_binario):
    '''
        Dato un codice binario di 4 numeri dove il primo è la prima settimana, il secondo è la seconda settimana etc.  
            1. restituisce il long dei mesi
            2. restituisce il conto delle settimane 
    '''
    #array_giorni=['Lun', 'Mar', 'Mer', 'Gio', 'Ven', 'Sab', 'Dom']
    long_mesi=''
    i=0
    conto_mesi=0
    while i<4:
        if codice_binario[i:(i+1)]=='1':
            conto_mesi+=1
            if long_mesi=='':
                long_mesi='{}'.format((i+1))
            else:
                long_mesi='{}-{}'.format(long_mesi,i+1)
        i+=1
    return long_mesi, conto_mesi


def codice_mensile(codice_binario1, codice_binario2):
    '''
        Dati i due codici binari restituisce il codice mensile
    '''
    cod=''
    s=0
    while s<4:
        if codice_binario2[s:(s+1)]=='1':
            i=0
            while i<7:
                if codice_binario1[i:(i+1)]=='1':
                    if cod=='':
                        cod='{}{}'.format(s+1,i+1)
                    else:
                        cod='{}_{}{}'.format(cod,s+1,i+1)
                i+=1 
        s+=1
    
    return cod


        


def main():
    '2006'
    codice_binario='011110011110'
    giorni=codice_binario[0:7]
    mesi=codice_binario[7:11]
    giorni_ok=giorni[::-1]
    freq_mensile_ok=mesi[::-1]
    mesi=long_mese(freq_mensile_ok)[0]
    num_mesi=long_mese(freq_mensile_ok)[1]
    cod_mese=codice_mensile(giorni_ok,freq_mensile_ok)
    print(mesi)
    print(num_mesi)
    print(cod_mese)
    
        
        
	    

if __name__ == "__main__":
    main()