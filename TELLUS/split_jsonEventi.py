#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2025
# Roberto Marzocchi


'''Ho un json molto grosso inviato da Tellus che andave in errore. 
Lo splitto e lancio il WS controllando che non vada in errore
'''

import os, sys, re  # ,shutil,glob
import inspect, os.path


import json


import logging


import requests


currentdir = os.path.dirname(os.path.realpath(__file__))
parentdir = os.path.dirname(currentdir)
sys.path.append(parentdir)
from credenziali import *

from invio_messaggio import *

path=os.path.dirname(sys.argv[0]) 
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




def main():
    logger.info('Il PID corrente è {0}'.format(os.getpid()))
    
    
    # Open and read the JSON file
    with open(f'{path}/input/amiu-data2.json', 'r') as file:
        data = json.load(file)

    # Print the data
    logger.debug(len(data))
    
    #logger.debug(data[0:10])
    
    
    
    step=10000
    
    url = 'https://amiugis.amiu.genova.it/ws_amiugis/InDettaglioEventi.php'
    
    
    i=0
    while i < len(data):
        # lancio il WS tellus per importare i dati
        if i+step<=len(data):
            x = requests.post(url, json = data[i:i+step])
        else: 
            x = requests.post(url, json = data[i:])
        if x.status_code!=200:
            logger.error(i)
            logger.error(x.status_code)
        else: 
            logger.info(x.status_code)
        #exit()
        i+=step
    
    
    
    
    # check se c_handller contiene almeno una riga 
    error_log_mail(errorfile, 'assterritorio@amiu.genova.it', os.path.basename(__file__), logger)

if __name__ == "__main__":
    main()