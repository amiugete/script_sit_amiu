#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2025
# Roberto Marzocchi, Roberta Fagandini

'''
Script che speriamo vivamente non serva più per corregggere le schede con tappe duplicate
per errori dovuti a itinerari che nel passato sono stati male inseriti su EKOVISION

Input: 
- lista percorsi con errori


Output 
- lista componente, data_inizo sbagliata che è da inviare a Ekovision per eliminazione di quelle componenti (almeno per il 2025)



SAREBBE DA GESTIRE IL FATTO CHE NON TUTTI I GIORNI CI SONO LE STESSE TAPPE IN FREQUENZA 
VEDI QUANTO FATTO PER LO SPAZZAMENTO 

'''

#from msilib import type_short
import os, sys, re  # ,shutil,glob
import inspect

import requests
from requests.exceptions import HTTPError

import json


#import getopt  # per gestire gli input

#import pymssql

from datetime import date, datetime, timedelta

import locale

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


from tappa_prevista import *

from crea_dizionario_da_query import *



filename = inspect.getframeinfo(inspect.currentframe()).filename
path=os.path.dirname(sys.argv[0]) 
path1 = os.path.dirname(os.path.dirname(os.path.abspath(filename)))
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
f_handler.setLevel(logging.INFO)


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
      


    
    logger.info('Il PID corrente è {0}'.format(os.getpid()))

    
    # Get today's date
    #presentday = datetime.now() # or presentday = datetime.today()
    oggi=datetime.today()
    oggi=oggi.replace(hour=0, minute=0, second=0, microsecond=0)
    oggi=date(oggi.year, oggi.month, oggi.day)
    #logging.debug('Oggi {}'.format(oggi))
    
    oggi_char=oggi.strftime('%Y%m%d')
    
    
    
    # credenziali WS Ekovision
    headers = {'Content-Type': 'application/x-www-form-urlencoded'}
    auth_data_eko={'user': eko_user, 'password': eko_pass, 'o2asp' :  eko_o2asp}
    
    

    
    # connessione a SIT
    nome_db=db
    logger.info('Connessione al db {}'.format(nome_db))
    conn = psycopg2.connect(dbname=nome_db,
                        port=port,
                        user=user,
                        password=pwd,
                        host=host)


    curr = conn.cursor()
    
    
    # Mi connetto al DB oracle UO
    cx_Oracle.init_oracle_client(percorso_oracle) # necessario configurare il client oracle correttamente
    #cx_Oracle.init_oracle_client() # necessario configurare il client oracle correttamente
    parametri_con='{}/{}@//{}:{}/{}'.format(user_uo,pwd_uo, host_uo,port_uo,service_uo)
    logger.debug(parametri_con)
    con = cx_Oracle.connect(parametri_con)
    logger.info("Versione ORACLE: {}".format(con.version))
    
    cur = con.cursor()
    
    
    
    # Percorsi solo raccolta (famiglia =1) con frequenza che abbiamo tirato fuori dallo script TREG/calendario.py
    percorsi_da_controllare_freq=['0101360101', '0101383101', '0507128002', '0500117901', '0501012809', '0501018002', '0502007402', '0500105503', '0303004201', '0500121602', '0501011909', '0507136902', '0500116101', '0101369101', '0500108503', '0507126502', '0101032202', '0508053202', '0507135101', '0101364301', '0500131601', '0508076002', '0507117201', '0500106201', '0508061801', '0101031901', '0101381203', '0507116601', '0507107003', '0507126401', '0502043501', '0101368601', '0101361702', '0508058901', '0101360502', '0501012909', '0101350901', '0508058201', '0101361001', '0507114301', '0501004001', '0101369301', '0501003801', '0101373901', '0507127502', '0507111701', '0500114701', '0101042102', '0508054201', '0500120501', '0101373701', '0101389901', '0507118302', '0502002703', '0500119301', '0501013009', '0500121901', '0507129201', '0101360903', '0508050603', '0508052701', '0101382603', '0500121803', '0507119202', '0501004501', '0501003601', '0507122101', '0101364801', '0501015109', '0501002901', '0507128203', '0101361301', '0500125402', '0508061701', '0508053302', '0502041201', '0500125103', '0501019501', '0501003002', '0508059202', '0101033502', '0501016701', '0101368501', '0507116501', '0502002803', '0101032702', '0508041101', '0101038401', '0500113601', '0507112102', '0501018801', '0101010601', '0507108001', '0508053702', '0501004601', '0507119602', '0500124602', '0101389001', '0501019401', '0500118703', '0500118201', '0101031503', '0507130103', '0507118401', '0101369901', '0500126403', '0502007103', '0101368401', '0104003501', '0101361502', '0508071501', '0501016801', '0101382503', '0500124903', '0501005701', '0501014609', '0101040101', '0507119702', '0508054001', '0500124702', '0500113101', '0101384101', '0101383901', '0101360602', '0101354603', '0500130802', '0101364901', '0502043703', '0501013909', '0101361401', '0508075602', '0101367601', '0501013209', '0508053002', '0500124501', '0508072201', '0101385201', '0500122801', '0101361201', '0500115601', '0507002202', '0507136602', '0500119103', '0507120201', '0508063902', '0101362801', '0101382403', '0101383601', '0508074601', '0500115801', '0508075702', '0507119901', '0507113502', '0508054902', '0508072001', '0101370201', '0101378303', '0500120101', '0501020002', '0501017602', '0101039201', '0507110301', '0101378101', '0501007301', '0508041801', '0101367901', '0507107603', '0101372101', '0500118801', '0101368301', '0501013409', '0501007501', '0101375601', '0101032903', '0101007101', '0501011809', '0500124001', '0502001603', '0501009001', '0508060302', '0500117501', '0508051101', '0501005401', '0508072301', '0500101002', '0507127602', '0101382103', '0500129101', '0500124402', '0500118001', '0501007102', '0502006402', '0101370401', '0101372801', '0501019902', '0101366301', '0507119402', '0507125201', '0507115501', '0502001703', '0500106301', '0500126501', '0101370801', '0101360001', '0508063203', '0508063501', '0507132603', '0500123301', '0507119102', '0501019301', '0500108102', '0508063303', '0102008201', '0500100402', '0101381501', '0101390202', '0508061401', '0508035701', '0501018202', '0500123102', '0101384001', '0502006801', '0501004801', '0101362601', '0507120301', '0101380101', '0501005201', '0507128702', '0101361602', '0507126202', '0101369701', '0507113701', '0101372201', '0101362903', '0508053102', '0500113401', '0507114801', '0508040501', '0507129803', '0507123503', '0508070801', '0101350803', '0101370901', '0101362501', '0101379102', '0500121703', '0501005901', '0500120302', '0500107202', '0501006101', '0500107702', '0500112501', '0508054402', '0501019201', '0501005301', '0101373301', '0101039401', '0508075401', '0500123702', '0507129401', '0508054301', '0508054802', '0101370001', '0101380801', '0507120001', '0500117001', '0500124302', '0101040503', '0500119901', '0508053402', '0508039303', '0507130601', '0502007201', '0508074802', '0501003102', '0501004301', '0508061901', '0303006709', '0101374201', '0500126101', '0507130201', '0508064202', '0508072101', '0501018401', '0101361101', '0500129603', '0508064303', '0508048701', '0508074702', '0507117703', '0507119302', '0501014009', '0500116501', '0101369401', '0508060102', '0501013309', '0507132703', '0501014109', '0500114801', '0501003901', '0101351001', '0508049901', '0502007003', '0501003701', '0500116001', '0501006001', '0501020901', '0508053602', '0507110601', '0508044501', '0101365501', '0507004701', '0502004002', '0501008601', '0507125401', '0101372901', '0508071802', '0508070901', '0507130502', '0501012509', '0101382303', '0507106202', '0101369501', '0508060403', '0507124601', '0101359602', '0500129703', '0501019601', '0101368001', '0101360802', '0101034202', '0508074401', '0101369001', '0507126302', '0500117601', '0101344301', '0500127903', '0507136402', '0508056601', '0101389101', '0101377901', '0500117701', '0101372701', '0501014909', '0508071001', '0501012009', '0500113301', '0507001902', '0500125003', '0101365101', '0508072401', '0501017101']
    
    
    # Percorsi solo raccolta (famiglia =1) senza frequenza
    # in questo caso li devo controllare tutti perchè non era possibile fare filtro con script TREG/calendario.py
    percorsi_da_controllare_nofreq=['0998003301', '0701006401', '0504009409', '0301006102', '0501012809', '0301005202', '0500125809', '0101044201', '0602006302', '0503001701', '0511000201', '0102002702', '0103012109', '0102000402', '0998002601', '0608003003', '0602004302', '0602005902', '0501011909', '0504007509', '0508025807', '0507134701', '0402000202', '0504007001', '0504007101', '0999001701', '0999003301', '0402000301', '0504006301', '0703002201', '0504007209', '0500133801', '0504005201', '0500133409', '0301006401', '0103007204', '0608003802', '0103006304', '0103005804', '0608006003', '0102006601', '0504007909', '0503001201', '0102000101', '0504008809', '0103002301', '0501015209', '0103011102', '0608006802', '0504010409', '0508077501', '0608004702', '0500119501', '0101047801', '0602005002', '0104003709', '0103008004', '0504009509', '0703003301', '0301003603', '0608005002', '0802001601', '0608003603', '0999003101', '0998003001', '0104003609', '0102006701', '0501012909', '0301005101', '0703003502', '0501011209', '0508075109', '0802000201', '0103009404', '0101385001', '0101046202', '0501010109', '0101379801', '0501012109', '0101387809', '0608006603', '0501016409', '0501009509', '0101045701', '0602004502', '0504010309', '0504004104', '0102006101', '0801001201', '0303007001', '0504008609', '0701006201', '0701006701', '0501012609', '0999003701', '0602004402', '0501013009', '0103008104', '0504009109', '0504006501', '0109002201', '0103007004', '0103006004', '0508055001', '0999003201', '0608003701', '0507138601', '0209000901', '0501015109', '0501010709', '0303007201', '0301002701', '0508000401', '0504003901', '0101387309', '0504005501', '0504008209', '0504004201', '0997003504', '0999004704', '0507131009', '0103009104', '0103006904', '0501014409', '0103010602', '0103006704', '1001111301', '0102007101', '0508074202', '0503000201', '0102006301', '0102007001', '0500101801', '0608003401', '0507136009', '0802003201', '0103007904', '0307001501', '0608005703', '0304002002', '0703001102', '0508069809', '0504008009', '0802002801', '0501013109', '0999002001', '0503000301', '0501012309', '0103009704', '0102008509', '0998003701', '0608005501', '0108000901', '0503002101', '0103003804', '0101388209', '0608001901', '0998000701', '0103011709', '0303007409', '0101381109', '0507004601', '0108001201', '0501011509', '0802000701', '0103009304', '0999002501', '0504004401', '0503001101', '0301006202', '0102006001', '0504006401', '0610000901', '0504008709', '0501012409', '0501009909', '0501020709', '0103010302', '0103008304', '1001100101', '0501020201', '0109000701', '0101382009', '0102008409', '0802003001', '0103009901', '0102005102', '0701006001', '0102007504', '0501011609', '0999000101', '0503001001', '0103007504', '0701006601', '0501014609', '0602006202', '0504008109', '0997003709', '0102008609', '0102004702', '0103008404', '0504006701', '0998003504', '0703000701', '0108001309', '0608004901', '0608001601', '0504006201', '0501014309', '0103008204', '0999003601', '0602004202', '0103007404', '0501012709', '0109001601', '0101352703', '0501013909', '0802001901', '0501016309', '0501014809', '0998002301', '0998001601', '0998000601', '0501013209', '0999002201', '0511000501', '0103010902', '0306002003', '0102008702', '0504007709', '0101044501', '0103004404', '0998001401', '0998000301', '0101387709', '0504005001', '0103012209', '0999003501', '0999001601', '0303007302', '0508028101', '0703001202', '0508001201', '0608000903', '0102007702', '0504007609', '0507138301', '0998001201', '0608003303', '0309000501', '0103000301', '0102007401', '0504006001', '0103009604', '0999003801', '0303003202', '0608002103', '0102002201', '0997003301', '0103007104', '0997003201', '0802001401', '0602005802', '0504008509', '0103012309', '0102002601', '0503001401', '0103006104', '0997003404', '0304001901', '0109001701', '0503000801', '0103007304', '0802001301', '0503000601', '0504009709', '0511001309', '0998003404', '0501020401', '0103001101', '0504009209', '0501013409', '0501012209', '0103005104', '0504006801', '0501011809', '0303006809', '0501010509', '0301006001', '0108000101', '0101388309', '0999001901', '0504004501', '0101387609', '0608004601', '0801002401', '0998002901', '0503001501', '0102006201', '0101388009', '0602006002', '0101392601', '0101385101', '0501010009', '0402000701', '0101388809', '0998002701', '0501010309', '0109001501', '0103009504', '0503001601', '0504004301', '0504005401', '0504005801', '0303005101', '0608003101', '0103001402', '0103002402', '0103005404', '0608002501', '0608000802', '0103011309', '0501006201', '0103004204', '0998003601', '0998003809', '0511000401', '0503001901', '0802001501', '0501014209', '0602005102', '0101387909', '0102007604', '0304002103', '0103009204', '0402000401', '0703000601', '0103006204', '0303007609', '0998001901', '0503000901', '0501013609', '0306001602', '0101381909', '0703000802', '0504008409', '0501014709', '0303006901', '0802003401', '0102004801', '0101387509', '0504007409', '0999002401', '0306002101', '0608003202', '0502005201', '0301003102', '0998003201', '0998002404', '0102006401', '0103011203', '0503001301', '0501011709', '0602004002', '0802001101', '0504010009', '0504010109', '0511000301', '0504008309', '0504006901', '0508077801', '0103006804', '0608006502', '0608001803', '0108001101', '0103006604', '0504004701', '0801001302', '0504009309', '0101045203', '0998001701', '0504007809', '0999002601', '0508074001', '0602004802', '0503000501', '0101379901', '0999000801', '0308000303', '0999000401', '0103011409', '0999004301', '0501009809', '0102002101', '0508074101', '0501009309', '0103010002', '0108000601', '0608005103', '0309000703', '0102004101', '0608001702', '0507134601', '0303007509', '0103004004', '0102002001', '0101379701', '0508001301', '0301006501', '0500002802', '0508075501', '0101387409', '0504006101', '0602005502', '0309000602', '0504005701', '0103012001', '0507121001', '0501010809', '0507136301', '0504004801', '0801000901', '0504009609', '0103010801', '0503000101', '0608002602', '0703003609', '0103004804', '0511001109', '0103005704', '0608006701', '0998002504', '0101047601', '0504008909', '0109000801', '0999002101', '0511000701', '0508060509', '0608005602', '0102008109', '0504005601', '0101388409', '0101349204', '0511000909', '0998001501', '0507135502', '0504004901', '0514000201', '0998000401', '0998001001', '0602005402', '0503002001', '0103011509', '0103001202', '0503000701', '0608004803', '0802002002', '0103008704', '0504004601', '0998001101', '0501014009', '0102000902', '0504009909', '0504007309', '0501010409', '0501020301', '0504010209', '0501011409', '0501013309', '0998002001', '0103007704', '0501014109', '0102004902', '0608006903', '0701006801', '0507128309', '0102007301', '0501009709', '0602005302', '0101044602', '0501010609', '0103001503', '0101379601', '0802002601', '0602005702', '0301006301', '0102006801', '0999004801', '0608003502', '0501015009', '0102007201', '0101391809', '0508064909', '0802003809', '0514000101', '0500127609', '0602006102', '0504005301', '0103001309', '0608006401', '0101389209', '0997003609', '0508027202', '0998000901', '0504006601', '0508069401', '0511001209', '0608002703', '0501010909', '0103007804', '0608001001', '0999000102', '0501012509', '0107000601', '0101388109', '0802001701', '0504004001', '0501010209', '0108000501', '0103011001', '0608003903', '0501009609', '0608002902', '0514000401', '0103008804', '0999001801', '0103009804', '0998002801', '0802002901', '0101389601', '0801002501', '0999001201', '0508069709', '0504005101', '0507004001', '0608001203', '0103004704', '0703000501', '0999004401', '0998001301', '1001110102', '0101374501', '0504009809', '0503001801', '0109001001', '0608001102', '0802002701', '0501014909', '0109002001', '0103011609', '0501014509', '0998003101', '0511000601', '0102006901', '0999002901', '0999002301', '0501012009', '0999001101', '0503000401', '0602005202', '0999003401', '0103009004', '0501013509', '0103003904', '0108000701', '0608000701', '0103008604', '0999003001', '0103007604', '0308000202', '0501020801', '0101343601', '0504009009', '0608005902', '0608005801', '0501020101', '0501011309', '0511001009', '0101380001', '0501009209', '0602004602', '0608002801', '0608002002', '0103008504', '0802003304', '0103010103', '1001111103']
    
    
    percorsi_da_controllare = percorsi_da_controllare_freq + percorsi_da_controllare_nofreq
    
    
    # il 23/10 abbiamo controllato anche tutti i percorsi di raccolta che non sono da gestire per ARERA 
    # e che quindi non avevamo tirato fuori dai test_calendario di TREG
    # sovrascriviamo la lista percorsi_da_controllare
    select_percorsi_da_controllare='''select distinct ep.cod_percorso from anagrafe_percorsi.elenco_percorsi ep 
join anagrafe_percorsi.anagrafe_tipo t on t.id  = ep.id_tipo 
where t.gestione_arera = 'f' and t.tipo_servizio = 'RACCOLTA' 
and ep.data_fine_validita >= now()'''

    try:
        curr.execute(select_percorsi_da_controllare)
        lista_percorsi=curr.fetchall()
    except Exception as e:
        logger.error(e)
        logger.error(select_percorsi_da_controllare)

    percorsi_da_controllare=[]
    
    for p in lista_percorsi:
        percorsi_da_controllare.append(p[0])
    logger.info(percorsi_da_controllare)
    #exit()
    
    query_variazioni_ekovision='''select 
codice_modello_servizio,
coalesce((select distinct ordine from anagrafe_percorsi.v_percorsi_elementi_tratti 
where codice_modello_servizio = tab.codice_modello_servizio 
and codice = tab.codice
and ripasso = tab.ripasso and data_fine is null limit 1),1)
as ordine,
objecy_type, 
  /*codice,*/
ce.id_ekovision,
  quantita, lato_servizio, percent_trattamento,
coalesce((select distinct frequenza from anagrafe_percorsi.v_percorsi_elementi_tratti 
where codice_modello_servizio = tab.codice_modello_servizio 
and codice = tab.codice
and ripasso = tab.ripasso and data_fine is null limit 1),0)
as 
  frequenza, 
  numero_passaggi, nota,
  codice_qualita, codice_tipo_servizio,
min(data_inizio) as data_inizio, 
/*case 
	when max(data_fine) = '99991231' then null 
	else max(data_fine)
end data_fine, 
*/
max(data_fine) as data_fine,
/*ripasso*/
case 
	when max(data_fine) = '99991231' then ripasso 
	else 0
end ripasso,
to_char(vspe.data_fine_validita, 'YYYYMMDD') as data_fine_validita
from (
	  SELECT codice_modello_servizio, ordine, objecy_type, 
  codice, quantita, lato_servizio, percent_trattamento,frequenza,
  ripasso, numero_passaggi, replace(replace(coalesce(nota,''),'DA PIAZZOLA',''),';', ' - ') as nota,
  codice_qualita, codice_tipo_servizio, data_inizio, coalesce(data_fine, '99991231') as data_fine
	 FROM anagrafe_percorsi.v_percorsi_elementi_tratti where data_inizio < coalesce(data_fine, '99991231')
	 union 
	   SELECT codice_modello_servizio, ordine, objecy_type, 
  codice, quantita, lato_servizio, percent_trattamento,frequenza,
  ripasso, numero_passaggi, replace(replace(coalesce(nota,''),'DA PIAZZOLA',''),';', ' - ') as nota,
  codice_qualita, codice_tipo_servizio, data_inizio, coalesce(data_fine, '99991231') as data_fine
	 FROM anagrafe_percorsi.v_percorsi_elementi_tratti_ovs where data_inizio < coalesce(data_fine, '99991231')	
	 union 
	 SELECT codice_modello_servizio, ordine, objecy_type, 
  codice, quantita, lato_servizio, percent_trattamento,frequenza,
  ripasso, numero_passaggi, replace(replace(coalesce(nota,''),'DA PIAZZOLA',''),';', ' - ') as nota,
  codice_qualita, codice_tipo_servizio, data_inizio, coalesce(data_fine, '99991231') as data_fine
	 FROM anagrafe_percorsi.mv_percorsi_elementi_tratti_dismessi where data_inizio < coalesce(data_fine, '99991231')
 ) tab 
 left join etl.componenti_ekovision ce on tab.codice = ce.id_elemento
 left join anagrafe_percorsi.v_servizi_per_ekovision vspe 
 	on vspe.cod_percorso = tab.codice_modello_servizio 
 	and vspe.versione = 
 	(select max(versione) from anagrafe_percorsi.v_servizi_per_ekovision vspe1 where vspe1.cod_percorso = vspe.cod_percorso)
 where codice_modello_servizio =%s 
 group by codice_modello_servizio,  objecy_type, 
  tab.codice,ce.id_ekovision, quantita, lato_servizio, percent_trattamento,
  ripasso, numero_passaggi, nota,
  codice_qualita, codice_tipo_servizio,
  vspe.data_fine_validita
  order by codice_modello_servizio, data_fine asc, ordine,  ripasso'''
  
  
  

    outputfile1='{0}/anomalie_output/{1}componenti_da_rimuovere.csv'.format(path,oggi_char)    
    f= open(outputfile1, "w")
    f.write('cod_percorso;id_componente_ekovision;data_inizio_sbagliata')
    
    
    
    # da usare per per
    for pdc in percorsi_da_controllare:
        logger.debug(pdc)
        
        componenti_OK = [] # quelle del sit
        
        componenti_eko=[]  #quelle di Ekovision
        ########################################################
        # EKOVISION 
        ########################################################
        
        id_scheda=0
        
        # cerco ultimo id_scheda con cui poi interrogherò i WS
        """
        query_id_scheda='''SELECT max(id_scheda) as ID_SCHEDA, 
        max(DATA_ESECUZIONE_PREVISTA ) as max_data 
        FROM SCHEDE_ESEGUITE_EKOVISION see 
        WHERE see.CODICE_SERV_PRED = :p1 
        AND see.RECORD_VALIDO = 'S'
        having max(id_scheda) IS NOT NULL'''
        """
        # cerco gli ultimi 7 id_scheda con cui poi interrogherò i WS
        query_id_scheda = '''
        SELECT a.* FROM (
        SELECT id_scheda as ID_SCHEDA, 
        DATA_ESECUZIONE_PREVISTA
        FROM SCHEDE_ESEGUITE_EKOVISION see 
        WHERE see.CODICE_SERV_PRED = :p1
        AND see.RECORD_VALIDO = 'S'
        ORDER BY 2 DESC) a
        WHERE rownum < 8
        '''
        try:
            cur.execute(query_id_scheda,(pdc,))
            max_id_scheda=cur.fetchall()
        except Exception as e:
            logger.error(e)
            logger.error(f'percorso : {pdc}')
            logger.error(query_id_scheda)
        
        for mdc in max_id_scheda:
            id_scheda=mdc[0]
            data_scheda=mdc[1]
        
        logger.debug(id_scheda)
        
        if id_scheda > 0:
        
            params={'obj':'schede_lavoro',
                        'act' : 'r',
                        'id': id_scheda
                        }
            response = requests.post(eko_url, params=params, data=auth_data_eko, headers=headers)
            #response.json()
            #logger.debug(response.status_code)
            check=0
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
                
                ss=0
                while ss < len(letture['schede_lavoro']):
                    trips=letture['schede_lavoro'][ss]['trips']
                    # ciclo sulle aste 
                    tr=0
                    while tr < len(trips):
                        waypoints=letture['schede_lavoro'][ss]['trips'][tr]['waypoints']
                        wid=0
                        while wid < len(waypoints):
                            works=letture['schede_lavoro'][ss]['trips'][tr]['waypoints'][wid]['works'] 
                            # ciclo sugli elementi
                            cc=0
                            while cc < len(works):
                                list=[]
                                list.append(int(letture['schede_lavoro'][ss]['trips'][tr]['waypoints'][wid]['works'][cc]['id_object']))
                                list.append(int(letture['schede_lavoro'][ss]['trips'][tr]['waypoints'][wid]['works'][cc]['data_inizio']))
                                list.append(int(letture['schede_lavoro'][ss]['trips'][tr]['waypoints'][wid]['works'][cc]['data_fine']))
                                componenti_eko.append(list)
                                cc+=1
                            wid+=1
                        tr+=1
                    ss+=1 
            
            logger.debug
            
            ########################################################
            # SIT 
            ########################################################
            
            try:
                curr.execute(query_variazioni_ekovision,(pdc,))
                dettaglio_percorso=curr.fetchall()
            except Exception as e:
                logger.error(e)
            

            
            
            for dpe in dettaglio_percorso:
                #logger.debug(dpe)
                list=[]
                #list.append(dpe[0])
                list.append(int(dpe[3]))
                list.append(int(dpe[12]))
                #if dpe[13]> data_scheda and dpe[13]!='99991231'and dpe[15]< oggi_char: # percorsi disattivi
                if dpe[13]>dpe[15]:
                    list.append(99991231)
                else:
                    list.append(int(dpe[13]))
                #list.append(int(dpe[14]))
                componenti_OK.append(list)
            
            #logger.debug(componenti_OK)
            
            
            
            logger.debug(componenti_OK)
            #exit()
            for ce in componenti_eko:
                if ce not in componenti_OK:
                    #logger.debug('{0};{1};{2}'.format(pdc, ce[0], ce[1]))
                    f.write('\n{0};{1};{2}'.format(pdc, ce[0], ce[1]))
            #exit()
    
    f.close()
        
    ##################################################################################################
    #                               CHIUDO LE CONNESSIONI
    ################################################################################################## 
    logger.info("Chiudo definitivamente le connesioni al DB")
    con.close()
    conn.close()

    # check se c_handller contiene almeno una riga 
    error_log_mail(errorfile, 'assterritorio@amiu.genova.it', os.path.basename(__file__), logger)

if __name__ == "__main__":
    main()
    