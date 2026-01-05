#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2024
# Roberto Marzocchi

'''
Pulire file log e non solo (anche pdf)
'''

import os,sys, getopt
import inspect, os.path


currentdir = os.path.dirname(os.path.realpath(__file__))
parentdir = os.path.dirname(currentdir)
sys.path.append(parentdir)

import time



#libreria per gestione log
import logging


#num_giorno=datetime.datetime.today().weekday()
#giorno=datetime.datetime.today().strftime('%A')






filename = inspect.getframeinfo(inspect.currentframe()).filename
#path = os.path.dirname(os.path.abspath(filename))
path1 = os.path.dirname(os.path.dirname(os.path.abspath(filename)))
path=os.path.dirname(sys.argv[0]) 
path1 = os.path.dirname(os.path.dirname(os.path.abspath(filename)))
nome=os.path.basename(__file__).replace('.py','')
#tmpfolder=tempfile.gettempdir() # get the current temporary directory
logfile='{0}/log/{1}.log'.format(path,nome)
errorfile='{0}/log/error_{1}.log'.format(path,nome)
#if os.path.exists(logfile):
#    os.remove(logfile)




# recupero la home
home_directory = os.path.expanduser("~")




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





now = time.time()




# VARIE
logpath=logfile='{}/log'.format(path)

variazioni='{}/variazioni'.format(path)

idea='{}/IDEA/output_file'.format(path)

ecopunti='{}/ecopunti'.format(path)

utenze='{}/utenze'.format(path)

#EKOVISION

preconsuntivazioni='{}/EKOVISION/preconsuntivazioni'.format(path)
consuntivazioni='{}/EKOVISION/consuntivazioni'.format(path)
timbrature='{}/EKOVISION/timbrature'.format(path)
assenze='{}/EKOVISION/assenze'.format(path)
eko_pesi='{}/EKOVISION/pesi'.format(path)

json_ekovision='{}/EKOVISION/eko_output'.format(path)

csv_ekovision_personale='{}/EKOVISION/inaz_output'.format(path)



# PERSONALE (Bruzzone/Rimunucci)
personale_ca_o='{}/personale/output/cartellini'.format(path)
personale_ce_o='{}/personale/output/cedolini'.format(path)
personale_cu_o='{}/personale/output/cu'.format(path)


#csv per comune 
csv_comune_ge='{}/csv_spazz_comune/'.format(path)

# report vari
report='{}/report'.format(path)

# cartella backup DB
backup_db= '{}/backup_db_new'.format(home_directory)


#TREG 
treg_log='{}/TREG/log'.format(path)

cartelle_da_pulire=[logpath, variazioni, idea, ecopunti, utenze, 
                    consuntivazioni, preconsuntivazioni, timbrature, assenze, json_ekovision,
                    personale_ca_o, personale_ce_o, personale_cu_o, csv_ekovision_personale, report,
                    eko_pesi, backup_db, csv_comune_ge, treg_log]

giorni_pulizia = [ 14, 14, 7, 14, 14,
          1, 7, 1, 1, 1,
          1, 1, 1, 7, 7,
          7, 7, 20, 7]

c=0
while c < len(cartelle_da_pulire):
    logging.info('Pulisco file nella cartella {}'.format(cartelle_da_pulire[c]))
    for f in os.listdir(cartelle_da_pulire[c]):
        if f not in ('README.md'):
            f = os.path.join(cartelle_da_pulire[c], f)
            if os.stat(f).st_mtime < now - giorni_pulizia[c] * 86400: 
                logger.debug(f)
                if os.path.isfile(f):
                    #print(f)
                    os.remove(os.path.join(path, f))
                if os.path.isdir(f):
                    logger.info('Guardo la sottocartella {}'.format(f))
                    logger.info(os.listdir(f))
                    for ff in os.listdir(f):
                        if ff not in ('README.md'):
                            ff = os.path.join(f, ff)
                            if os.stat(ff).st_mtime < now - giorni_pulizia[c] * 86400: 
                                if os.path.isfile(ff):
                                    #print(f)
                                    os.remove(os.path.join(path, ff)) 
            if os.path.isdir(f):
                if len(os.listdir(f))==0:
                    logger.info('Directory {} vuota. La rimuovo'.format(f))
                    os.rmdir(f)                
                              
    logger.debug('Sono arrivato qua')
    c+=1
