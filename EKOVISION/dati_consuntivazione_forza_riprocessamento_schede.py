#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2023
# Roberto Marzocchi

'''
INPUT 
- una query specifica che restituisce un elenco di ID_SCHEDE 
- elenco ID_SCHEDE


forzo il salvataggio della scheda Ekovision per fare in modo che i dati vengano riprocessati da AMIU


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

import uuid

    
     

def main():
      



    
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
    
    
    '''
    #QUERY
    # spazzamemto 
    select_schede="""SELECT DISTINCT id_scheda FROM 
(
	SELECT see.id_scheda, see.NOMEFILE, ar.* FROM 
		(/* casi anomali*/
		SELECT giorno, esito, id_percorso, descrizione_percorso, servizio, via, rsxd.nota_via, ambito, comune, municipio, turno, via_prevista, 
		percentuale_completamento
		FROM REPORT_SPAZZ_X_DUALE rsxd
		                      LEFT JOIN CONS_MACRO_TAPPA cmt
		                         ON cmt.ID_MACRO_TAPPA = rsxd.ID_TAPPA
		                      LEFT JOIN strade.ASTE a ON a.ID_ASTA = rsxd.ID_ASTA
		WHERE via_prevista = 'SI'
		   AND PERCENTUALE_COMPLETAMENTO<>100 
		   AND ESITO NOT LIKE 'ANTICIPATO%'
		   AND ESITO NOT LIKE 'POSTICIPATO%'
		   AND NVL (tempo_recupero, 25) > 24
		   AND COALESCE(ID_CAUSALE_ARERA, 0) = 0
		 ORDER BY giorno
		) ar 
		LEFT JOIN SCHEDE_ESEGUITE_EKOVISION see ON to_date(see.DATA_ESECUZIONE_PREVISTA, 'YYYYMMDD') = ar.giorno
		AND see.CODICE_SERV_PRED = ar.ID_PERCORSO AND see.RECORD_VALIDO = 'S'
	/*JOIN CONSUNT_EKOVISION_SPAZZAMENTO ces ON to_date(ces.DATA_ESECUZIONE_PREVISTA, 'YYYYMMDD') = ar.giorno
	AND ces.CODICE_SERV_PRED = ar.ID_PERCORSO AND ces.RECORD_VALIDO = 'S' */
) WHERE id_scheda IS NOT null
"""


    # raccolta
    select_schede="""/*RACCOLTA*/
SELECT id_scheda FROM (
	SELECT DISTINCT id_scheda,
	giorno, esito, id_percorso 
	FROM 
	(
		SELECT see.id_scheda, see.NOMEFILE, ar.* FROM 
			(/* casi anomali*/
			SELECT giorno, esito, rrxd.id_percorso, rrxd.descrizione_percorso, servizio, 
			via, civico, ambito, comune, municipio, turno, tappa_prevista
			FROM REPORT_RACCOLTA_X_DUALE rrxd
			JOIN anagr_ser_per_uo aspu ON rrxd.id_percorso = aspu.ID_PERCORSO 
										and rrxd.GIORNO BETWEEN aspu.dta_attivazione AND aspu.dta_disattivazione
			JOIN anagr_uo au ON au.id_UO = aspu.id_UO 
	          LEFT JOIN CONS_MACRO_TAPPA cmt
	             ON cmt.ID_MACRO_TAPPA = rrxd.ID_TAPPA
			WHERE NVL(au.DITTA_ESTERNA, 'N') !='S'
			AND tappa_prevista = 'SI'
			   AND NUMERO_CONTENITORI_VUOTATI = 0 
			   AND ESITO NOT LIKE 'ANTICIPATO%'
			   AND ESITO NOT LIKE 'POSTICIPATO%'
			   AND NVL(tempo_recupero, 25) > 24
			   AND COALESCE(ID_CAUSALE_ARERA, 0) = 0
			   AND esito = 'NON CONSUNTIVATO' AND TO_char(giorno, 'YYYY')= 2024
			 ORDER BY giorno
			) ar 
			LEFT JOIN SCHEDE_ESEGUITE_EKOVISION see ON to_date(see.DATA_ESECUZIONE_PREVISTA, 'YYYYMMDD') = ar.giorno
			AND see.CODICE_SERV_PRED = ar.ID_PERCORSO AND see.RECORD_VALIDO = 'S'
		/*JOIN CONSUNT_EKOVISION_SPAZZAMENTO ces ON to_date(ces.DATA_ESECUZIONE_PREVISTA, 'YYYYMMDD') = ar.giorno
		AND ces.CODICE_SERV_PRED = ar.ID_PERCORSO AND ces.RECORD_VALIDO = 'S' */
	) /*WHERE id_scheda IS NOT NULL*/
	ORDER BY giorno
) WHERE id_scheda IS NOT NULL"""    
    
   ''' 
    
    # raccolta in cui è stato fatto in parte il lavaggio ma dove non ci sono i dati sulla consunt_macro_tappa  
    select_schede= """SELECT DISTINCT ID_SCHEDA, CODICE_SERV_PRED, DATA_ESECUZIONE_PREVISTA, b.ID_PERCORSO
FROM CONSUNT_EKOVISION_RACCOLTA cer
LEFT JOIN (SELECT DISTINCT
ID_PERCORSO, to_char(data_cons, 'YYYYMMDD') AS data_c 
FROM CONSUNT_MACRO_TAPPA cmt WHERE CAUSALE_ELEM = 110) b
	ON concat(cer.CODICE_SERV_PRED, DATA_ESECUZIONE_PREVISTA) = concat(b.ID_PERCORSO, b.DATA_C)
WHERE cer.CAUSALE =110 AND b.ID_PERCORSO IS NULL
ORDER BY 3"""
    
    try:
        cur.execute(select_schede)
        check_schede=cur.fetchall()
    except Exception as e:
        logger.error(select_schede)
        logger.error(e)
    
    
    
    # inserimento manuale id_scheda
    #check_schede=[[483442],[483443]]
    # [382388],  [388680], [395036]
    
    ################################
    # ATTENZIONE ORA è su TEST (da cambiare 2 volte l'URL (lettura e scrittura) 
    
    
    #check_schede=[[576939]] 
    check_schede=[
        [636034],
[481241],
[485006],
[487079],
[504487],
[507199],
[514991],
[516271],
[536618],
[539660],
[540261],
[541128],
[544611],
[546066],
[546067],
[563182],
[563441],
[569353],
[574351],
[578859],
[581037],
[581465],
[584835],
[586162],
[596498],
[604842],
[608905],
[609214],
[616106],
[616175],
[617035],
[625217],
[626280],
[629677],
[629678],
[630059],
[633416],
[478967],
[491467],
[498621],
[499399],
[500987],
[502658],
[627711],
[573242],
[496735],
[573325],
[602553],
[603136],
[549962],
[561642],
[603254],
[620387],
[490528],
[609064],
[502332],
[502367],
[508213],
[508699],
[514608],
[626922],
[584929],
[525453],
[526328],
[627674],
[538349],
[596503],
[596912],
[596918],
[609039],
[632199],
[526011],
[619502],
[579724],
[503913] 
    ]
    id_schede_problemi=[]
    for id_scheda in check_schede:
     
    
   
        logger.info('Provo a leggere i dettagli della scheda {}'.format(id_scheda[0]))
        
        
        params2={'obj':'schede_lavoro',
                'act' : 'r',
                'id': '{}'.format(id_scheda[0]),
                'flg_esponi_consunt' : 1
                }
        
        response2 = requests.post(eko_url, params=params2, data=data, headers=headers)
        #letture2 = response2.json()
        #try: 
        letture2 = response2.json()
        #logger.info(letture2)
        #exit()
        # key to remove
        #key_to_remove = "status"
        del letture2["status"]  
        del letture2['schede_lavoro'][0]['trips']  
        del letture2['schede_lavoro'][0]['risorse_tecniche']
        del letture2['schede_lavoro'][0]['risorse_umane']
        del letture2['schede_lavoro'][0]['serv_conferimenti']
        del letture2['schede_lavoro'][0]['filtri_rfid']        
        #logger.info(letture2)
        #exit()
        #logger.info(json.dumps(letture2).encode("utf-8"))
        
        
        
        
        
        
        
        
        logger.info('Provo a salvare nuovamente la scheda {}'.format(id_scheda[0]))
        
        
        guid = uuid.uuid4()
        params2={'obj':'schede_lavoro',
                'act' : 'w',
                'ruid': '{}'.format(str(guid)),
                'json': json.dumps(letture2, ensure_ascii=False).encode('utf-8')
                }
        #exit()
        response2 = requests.post(eko_url, params=params2, data=data, headers=headers)
        try:
            result2 = response2.json()
            if result2['status']=='error':
                logger.error('Id_scheda = {}'.format(id_scheda))
                logger.error(result2)
        except Exception as e:
            logger.error(e)
            warning_message_mail('Problema scheda {}'.format(id_scheda[0]), 'roberto.marzocchi@amiu.genova.it', os.path.basename(__file__), logger)
        
        
        #logger.info('Fatto')
    #else :
    #    logger.info(result2['status'])
    
    '''try: 
        id_scheda=letture['crea_schede_lavoro'][0]['id']
    except Exception as e:
        logger.error(e)
    '''




if __name__ == "__main__":
    main()      