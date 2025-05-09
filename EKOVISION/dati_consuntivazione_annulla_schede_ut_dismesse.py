#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2023
# Roberto Marzocchi

'''
Data una query specifica che restituisce un elenco di ID_SCHEDE forzo il salvataggio della scheda Ekovision per fare in modo che i dati vengano riprocessati da AMIU


'''

#from msilib import type_short
import os, sys, re  # ,shutil,glob

import requests
from requests.exceptions import HTTPError

import json


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


#import requests

import logging


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



    
     

def main():
      


    

    
    test= {"name": "école '& c/o aaa", 
        "location": "New York"}
    
    json_data = json.dumps(test , ensure_ascii=False).encode('utf-8')
    
    print(json_data)
    
    #exit()
    
    # Get today's date
    #presentday = datetime.now() # or presentday = datetime.today()
    oggi=datetime.today()
    oggi=oggi.replace(hour=0, minute=0, second=0, microsecond=0)
    oggi=date(oggi.year, oggi.month, oggi.day)
    logging.debug('Oggi {}'.format(oggi))
    
    
    check=0
    
    headers = {'Content-Type': 'application/x-www-form-urlencoded'}
    
    #headers = {'Content-type': 'application/json;'}

    data={'user': eko_user, 
        'password': eko_pass,
        'o2asp' :  eko_o2asp
        }
    
    
    
    # Mi connetto al DB oracle UO
    cx_Oracle.init_oracle_client(percorso_oracle) # necessario configurare il client oracle correttamente
    #cx_Oracle.init_oracle_client() # necessario configurare il client oracle correttamente
    parametri_con='{}/{}@//{}:{}/{}'.format(user_uo,pwd_uo, host_uo,port_uo,service_uo)
    logger.debug(parametri_con)
    con = cx_Oracle.connect(parametri_con)
    logger.info("Versione ORACLE: {}".format(con.version))
    
    cur = con.cursor()
    
    
    
    # inserimento manuale id_scheda
    #check_schede=[[483442],[483443]]
    check_schede=[[350322],[ 356627],[ 381872],[ 381893],[ 394384],[ 394385],[ 394389],[ 394380],[ 394390],[ 403789],[ 403790],[ 403801],[ 403791],[ 403792],[ 403802],[ 403793],[ 403794],[ 403803],[ 403795],[ 403796],[ 403804],[ 403797],[ 403798],[ 403805],[ 403799],[ 403800],[ 403806],[ 409718],[ 409719],[ 409730],[ 409720],[ 409721],[ 409731],[ 418449],[ 409722],[ 409723],[ 409732],[ 418518],[ 409724],[ 409725],[ 409733],[ 418517],[ 418516],[ 409726],[ 409727],[ 409734],[ 409728],[ 409729],[ 409735],[ 415590],[ 415591],[ 415602],[ 415592],[ 415593],[ 415603],[ 415594],[ 415595],[ 415604],[ 415596],[ 415597],[ 415605],[ 415598],[ 415599],[ 415606],[ 415600],[ 415601],[ 415607],[ 421501],[ 421502],[ 421513],[ 421503],[ 421504],[ 421514],[ 421505],[ 421506],[ 421515],[ 424176],[ 424180],[ 424171],[ 424177],[ 424170],[ 421507],[ 421508],[ 421516],[ 424181],[ 424172],[ 424175],[ 424173],[ 424182],[ 424174],[ 424178],[ 424183],[ 424184],[ 424169],[ 424179],[ 421509],[ 421510],[ 421517],[ 421511],[ 421512],[ 421518],[ 427343],[ 427344],[ 427355],[ 427345],[ 427346],[ 427356],[ 427347],[ 427348],[ 427357],[ 427349],[ 427350],[ 427358],[ 427351],[ 427352],[ 427359],[ 427353],[ 427354],[ 427360],[ 433396],[ 433397],[ 433408],[ 433398],[ 433399],[ 433409],[ 433400],[ 433401],[ 433410],[ 433402],[ 433403],[ 433411],[ 433404],[ 433405],[ 433412],[ 433406],[ 433407],[ 433413],[ 439299],[ 439300],[ 439311],[ 439301],[ 439302],[ 439312],[ 439303],[ 439304],[ 439313],[ 439305],[ 439306],[ 439314],[ 439307],[ 439308],[ 439315],[ 439309],[ 439310],[ 439316],[ 445225],[ 445226],[ 445237],[ 445227],[ 445228],[ 445238],[ 445229],[ 445230],[ 445239],[ 445231],[ 445232],[ 445240],[ 445233],[ 445234],[ 445241],[ 445235],[ 445236],[ 445242],[ 451143],[ 451144],[ 451155],[ 451145],[ 451146],[ 451156],[ 451147],[ 451148],[ 451157],[ 451149],[ 451150],[ 451158],[ 451151],[ 451152],[ 451159],[ 451153],[ 451154],[ 451160],[ 457062],[ 457063],[ 457074],[ 457064],[ 457065],[ 457075],[ 457066],[ 457067],[ 457076],[ 457068],[ 457069],[ 457077],[ 457070],[ 457071],[ 457078],[ 457072],[ 457073],[ 457079],[ 462992],[ 462993],[ 463004],[ 462994],[ 462995],[ 463005],[ 462996],[ 462997],[ 463006],[ 462998],[ 462999],[ 463007],[ 463000],[ 463001],[ 463008],[ 463002],[ 463003],[ 463009],[ 468894],[ 468895],[ 468906],[ 468896],[ 468897],[ 468907],[ 468898],[ 468899],[ 468908],[ 468900],[ 468901],[ 468909],[ 468902],[ 468903],[ 468910],[ 468904],[ 468905],[ 468911],[ 474777],[ 474778],[ 474789],[ 474779],[ 474780],[ 474790],[ 477427],[ 474781],[ 474782],[ 474791],[ 474783],[ 474784],[ 474792],[ 474785],[ 474786],[ 474793],[ 474787],[ 474788],[ 474794],[ 477482],[ 480647],[ 480648],[ 480651],[ 480649],[ 480650],[ 480652],[ 483277],[ 483262],[ 483272]]
    
    
    for id_scheda in check_schede:
    
    
    
    
    
    

    
    
    
    
    
   
        logger.info('Provo a leggere i dettagli della scheda {}'.format(id_scheda[0]))
        
        
        params2={'obj':'schede_lavoro',
                'act' : 'r',
                'id': '{}'.format(id_scheda[0]),
                }
        
        response2 = requests.post(eko_url, params=params2, data=data, headers=headers)
        #letture2 = response2.json()
        letture2 = response2.json()
        #logger.info(letture2)
        #exit()
        # key to remove
        #key_to_remove = "status"
        del letture2["status"]  
        del letture2['schede_lavoro'][0]['trips']  
        del letture2['schede_lavoro'][0]['risorse_tecniche']
        del letture2['schede_lavoro'][0]['risorse_umane']
        del letture2['schede_lavoro'][0]['filtri_rfid']        
        #logger.info(letture2)
        
        #logger.info(json.dumps(letture2).encode("utf-8"))
        
        
        
        letture2['schede_lavoro'][0]['servizi'][0]['flg_segn_srv_non_effett']="1"
        letture2['schede_lavoro'][0]['servizi'][0]['txt_segn_srv_non_effett']="U.T. non soggetta a consuntivazione su Ekovision"
        letture2['schede_lavoro'][0]['servizi'][0]['id_caus_srv_non_eseg']='15'
        letture2['schede_lavoro'][0]['flg_eseguito']='1'
        letture2['schede_lavoro'][0]['flg_imposta_eseguito']='1'
        
        
        
        
        logger.info('Provo a salvare nuovamente la scheda {}'.format(id_scheda[0]))
        
        
        
        params2={'obj':'schede_lavoro',
                'act' : 'w',
                'ruid': 'B{}'.format(id_scheda[0]),
                'json': json.dumps(letture2, ensure_ascii=False).encode('utf-8')
                }
        #exit()
        response2 = requests.post(eko_url, params=params2, data=data, headers=headers)
        result2 = response2.json()
        if result2['status']=='error':
            logger.error('Id_scheda = {}'.format(id_scheda))
            logger.error(result2)
    #else :
    #    logger.info(result2['status'])
    
    '''try: 
        id_scheda=letture['crea_schede_lavoro'][0]['id']
    except Exception as e:
        logger.error(e)
    '''




if __name__ == "__main__":
    main()      