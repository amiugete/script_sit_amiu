#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2021
# Roberto Marzocchi

'''
Pulire file log
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

cartelle_da_pulire=[logpath, variazioni, idea, ecopunti, utenze, consuntivazioni, preconsuntivazioni, timbrature, assenze]


c=0
while c < len(cartelle_da_pulire):
    logging.info('Pulisco file nella cartella {}'.format(cartelle_da_pulire[c]))
    for f in os.listdir(cartelle_da_pulire[c]):
        if f not in ('README.md'):
            f = os.path.join(cartelle_da_pulire[c], f)
            if os.stat(f).st_mtime < now - 14 * 86400: 
                if os.path.isfile(f):
                    #print(f)
                    os.remove(os.path.join(path, f))
    logging.debug('Sono arrivato qua')
    c+=1
