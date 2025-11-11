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

import uuid

    
     

def main():
      


    
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
    check_schede=[
        [618548], [618549], [618551], [618552], [618550], [624433], [624425], [624429], [624426], [624427], [624428], [624432], [624430], [624407], [624424], [624423], [624408], [624409], [624410], [624419], [624431], [624411], [624403], [624417], [624418], [624412], [624404], [624413], [624414], [624415], [624405], [624416], [624406], [624421], [624422], [624420], [624464], [624456], [624460], [624457], [624458], [624459], [624463], [624461], [624438], [624455], [624454], [624439], [624440], [624441], [624450], [624462], [624442], [624434], [624448], [624449], [624443], [624435], [624444], [624445], [624446], [624436], [624447], [624437], [624452], [624453], [624451], [624495], [624487], [624491], [624488], [624489], [624490], [624494], [624492], [624469], [624486], [624485], [624470], [624471], [624472], [624481], [624493], [624473], [624465], [624479], [624480], [624474], [624466], [624475], [624476], [624477], [624467], [624478], [624468], [624483], [624484], [624482], [624526], [624518], [624522], [624519], [624520], [624521], [624525], [624523], [624500], [624517], [624516], [624501], [624502], [624503], [624512], [624524], [624504], [624496], [624510], [624511], [624505], [624497], [624506], [624507], [624508], [624498], [624509], [624499], [624514], [624515], [624513], [624557], [624549], [624553], [624550], [624551], [624552], [624556], [624554], [624531], [624548], [624547], [624532], [624533], [624534], [624543], [624555], [624535], [624527], [624541], [624542], [624536], [624528], [624537], [624538], [624539], [624529], [624540], [624530], [624545], [624546], [624544], [624586], [624579], [624580], [624581], [624582], [624585], [624583], [624562], [624578], [624577], [624563], [624565], [624564], [624573], [624584], [624566], [624558], [624571], [624572], [624567], [624559], [624568], [624569], [624560], [624570], [624561], [624575], [624576], [624574], [624587], [624588], [624590], [624591], [624589], [630418], [630410], [630414], [630411], [630412], [630413], [630417], [630415], [630392], [630409], [630408], [630393], [630394], [630395], [630404], [630416], [630396], [630388], [630402], [630403], [630397], [630389], [630398], [630399], [630400], [630390], [630401], [630391], [630406], [630407], [630405], [630449], [630441], [630445], [630442], [630443], [630444], [630448], [630446], [630423], [630440], [630439], [630424], [630425], [630426], [630435], [630447], [630427], [630419], [630433], [630434], [630428], [630420], [630429], [630430], [630431], [630421], [630432], [630422], [630437], [630438], [630436], [630480], [630472], [630476], [630473], [630474], [630475], [630479], [630477], [630454], [630471], [630470], [630455], [630456], [630457], [630466], [630478], [630458], [630450], [630464], [630465], [630459], [630451], [630460], [630461], [630462], [630452], [630463], [630453], [630468], [630469], [630467], [630511], [630503], [630507], [630504], [630505], [630506], [630510], [630508], [630485], [630502], [630501], [630486], [630487], [630488], [630497], [630509], [630489], [630481], [630495], [630496], [630490], [630482], [630491], [630492], [630493], [630483], [630494], [630484], [630499], [630500], [630498], [630542], [630534], [630538], [630535], [630536], [630537], [630541], [630539], [630516], [630533], [630532], [630517], [630518], [630519], [630528], [630540], [630520], [630512], [630526], [630527], [630521], [630513], [630522], [630523], [630524], [630514], [630525], [630515], [630530], [630531], [630529], [630571], [630564], [630565], [630566], [630567], [630570], [630568], [630547], [630563], [630562], [630548], [630550], [630549], [630558], [630569], [630551], [630543], [630556], [630557], [630552], [630544], [630553], [630554], [630545], [630555], [630546], [630560], [630561], [630559], [630572], [630573], [630575], [630576], [630574], [636421], [636413], [636417], [636414], [636415], [636416], [636420], [636418], [636395], [636412], [636411], [636396], [636397], [636398], [636407], [636419], [636399], [636391], [636405], [636406], [636400], [636392], [636401], [636402], [636403], [636393], [636404], [636394], [636409], [636410], [636408], [636452], [636444], [636448], [636445], [636446], [636447], [636451], [636449], [636426], [636443], [636442], [636427], [636428], [636429], [636438], [636450], [636430], [636422], [636436], [636437], [636431], [636423], [636432], [636433], [636434], [636424], [636435], [636425], [636440], [636441], [636439], [636483], [636475], [636479], [636476], [636477], [636478], [636482], [636480], [636457], [636474], [636473], [636458], [636459], [636460], [636469], [636481], [636461], [636453], [636467], [636468], [636462], [636454], [636463], [636464], [636465], [636455], [636466], [636456], [636471], [636472], [636470], [636514], [636506], [636510], [636507], [636508], [636509], [636513], [636511], [636488], [636505], [636504], [636489], [636490], [636491], [636500], [636512], [636492], [636484], [636498], [636499], [636493], [636485], [636494], [636495], [636496], [636486], [636497], [636487], [636502], [636503], [636501], [636545], [636537], [636541], [636538], [636539], [636540], [636544], [636542], [636519], [636536], [636535], [636520], [636521], [636522], [636531], [636543], [636523], [636515], [636529], [636530], [636524], [636516], [636525], [636526], [636527], [636517], [636528], [636518], [636533], [636534], [636532], [636574], [636567], [636568], [636569], [636570], [636573], [636571], [636550], [636566], [636565], [636551], [636553], [636552], [636561], [636572], [636554], [636546], [636559], [636560], [636555], [636547], [636556], [636557], [636548], [636558], [636549], [636563], [636564], [636562], [636575], [636576], [636578], [636579], [636577], [642472], [642464], [642468], [642465], [642466], [642467], [642471], [642469], [642446], [642463], [642462], [642447], [642448], [642449], [642458], [642470], [642450], [642442], [642456], [642457], [642451], [642443], [642452], [642453], [642454], [642444], [642455], [642445], [642460], [642461], [642459]
          ]    
    
    
    
    
    
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
        letture2['schede_lavoro'][0]['servizi'][0]['txt_segn_srv_non_effett']="UT non soggetta a consuntivazione o situazione da chiarire con COGE"
        letture2['schede_lavoro'][0]['servizi'][0]['id_caus_srv_non_eseg']='15'
        letture2['schede_lavoro'][0]['flg_eseguito']='1'
        letture2['schede_lavoro'][0]['flg_imposta_eseguito']='1'
        
        
        
        
        logger.info('Provo a salvare nuovamente la scheda {}'.format(id_scheda[0]))
        
        
        guid = uuid.uuid4()

        params2={'obj':'schede_lavoro',
                'act' : 'w',
                'ruid': '{}'.format(str(guid)),
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