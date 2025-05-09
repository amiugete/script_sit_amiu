#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2023
# Roberto Marzocchi

'''
Data una query specifica forzo il salvataggio della scheda Ekovision per fare in modo che i dati vengano riprocessati da AMIU


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
    
    cod_percorso=[]
    data_percorso=[]
    stato_cons=[]
    n_schede=[]
    
    
    #QUERY
    
    # spazzamento
    select_schede="""
    SELECT DISTINCT id_scheda,
to_char(giorno, 'YYYYMMDD') as giorno, esito, id_percorso
FROM 
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
        and to_char(giorno, 'YYYY')='2024'
		 ORDER BY giorno
		) ar 
		LEFT JOIN SCHEDE_ESEGUITE_EKOVISION see ON to_date(see.DATA_ESECUZIONE_PREVISTA, 'YYYYMMDD') = ar.giorno
		AND see.CODICE_SERV_PRED = ar.ID_PERCORSO AND see.RECORD_VALIDO = 'S'
	/*JOIN CONSUNT_EKOVISION_SPAZZAMENTO ces ON to_date(ces.DATA_ESECUZIONE_PREVISTA, 'YYYYMMDD') = ar.giorno
	AND ces.CODICE_SERV_PRED = ar.ID_PERCORSO AND ces.RECORD_VALIDO = 'S' */
) /*WHERE id_scheda IS NOT NULL*/
ORDER BY giorno
    """
    
    
    
    # raccolta
    select_schede="""
    SELECT DISTINCT id_scheda,
	to_char(giorno, 'YYYYMMDD') as giorno, esito, id_percorso 
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
    """
    
    
    
    try:
        cur.execute(select_schede)
        check_schede=cur.fetchall()
    except Exception as e:
        logger.error(select_schede)
        logger.error(e)
        
        
        
        
    for id_scheda in check_schede:
    
  
        logger.info('Provo a leggere i dettagli della scheda {}'.format(id_scheda[0]))
        
        
        
        cod_percorso.append(id_scheda[1])
        data_percorso.append(id_scheda[3])
        params={'obj':'schede_lavoro',
                    'act' : 'r',
                    'sch_lav_data': id_scheda[1],
                    'cod_modello_srv': id_scheda[3],
                    'flg_includi_eseguite': 1,
                    'flg_includi_chiuse': 1
                    }
        try:
            #requests.Cache.remove(eko_url)
            response = requests.post(eko_url, headers=headers, params=params, data=data)
        except Exception as err:
            logger.error(f'Errore in connessione: {err}')
            error_log_mail(errorfile, 'roberto.marzocchi@amiu.genova.it', os.path.basename(__file__), logger)
            logger.info("chiudo le connessioni in maniera definitiva")
            cur.close()
            con.close()
            exit()
        #response.json()
        #logger.debug(response.status_code)
        try:      
            response.raise_for_status()
            check=0
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
            #logger.info(letture)
            #exit()
            n_schede.append(len(letture['schede_lavoro']))
            flg_eseguito=''
            if len(letture['schede_lavoro']) == 1 : 
                id_scheda=letture['schede_lavoro'][0]['id_scheda_lav']
                flg_eseguito=letture['schede_lavoro'][0]['flg_eseguito']
                #logger.info('Id_scheda:{}'.format(id_scheda))
            elif len(letture['schede_lavoro']) > 1 : 
                j=0
                while j < len(letture['schede_lavoro']):
                    id_scheda=letture['schede_lavoro'][j]['id_scheda_lav']
                    flg_eseguito='{} {}'.format(flg_eseguito, letture['schede_lavoro'][0]['flg_eseguito']) 
                    j+=1
                
            stato_cons.append(flg_eseguito)
        
        
        else: 
            n_schede.append(0)
            stato_cons.append(0)
        
        
    
    logger.info(len(n_schede))
    logger.info(len(stato_cons))
    logger.info(len(cod_percorso))   
    logger.info(len(data_percorso)) 
    try:    
        nome_csv_ekovision="controllo_schede_non_consuntivate.csv"
        file_output_ekovision="{0}/{1}".format(path,nome_csv_ekovision)
        fp = open(file_output_ekovision, 'w', encoding='utf-8')
                    
        fieldnames = ['cod_percorso', 'data', 'num_schede', 'stato_cons']
    
    
        myFile = csv.writer(fp, delimiter=';')
        myFile.writerow(fieldnames)
        
        k=0 
        while k < len(cod_percorso):
            row=[cod_percorso[k], data_percorso[k], n_schede[k], stato_cons[k]]
            myFile.writerow(row)
            k+=1
        '''
        matrice=[tuple(cod_percorso), tuple(data), tuple(id_turno), tuple(id_componente),tuple(id_tratto),
                        tuple(flag_esecuzione), tuple(causale), tuple(nota_causale), tuple(sorgente_dati), tuple(data_ora), tuple(lat), tuple(long)]
        myFile.writerows(matrice)
        '''
        fp.close()
    except Exception as e:
        logger.error('Problema creazione file CSV')
        logger.error(e)
        check_ekovision=102 # problema file variazioni



if __name__ == "__main__":
    main()      