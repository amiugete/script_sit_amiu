#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2021
# Roberto Marzocchi


import datetime

import holidays
from workalendar.europe import Italy

import logging

import os
import inspect, os.path

filename = inspect.getframeinfo(inspect.currentframe()).filename
path     = os.path.dirname(os.path.abspath(filename))

logfile='{}/log/variazioni.log'.format(path)
#if os.path.exists(logfile):
#    os.remove(logfile)

logging.basicConfig(format='%(asctime)s\t%(levelname)s\t%(message)s',
    #filemode='a', # overwrite or append
    #filename=logfile,
    level=logging.DEBUG)


def main():
    # carico i mezzi sul DB PostgreSQL


    oggi=datetime.datetime.today()
    oggi=oggi.replace(hour=0, minute=0, second=0, microsecond=0) 
    logging.debug('Oggi {}'.format(oggi))
    
    
    num_giorno=datetime.datetime.today().weekday()
    giorno=datetime.datetime.today().strftime('%A')
    giorno_file=datetime.datetime.today().strftime('%Y%m%d')
    logging.debug('Il giorno della settimana è {} o meglio {}'.format(num_giorno, giorno))
    
    
    
    holiday_list = []
    holiday_list_pulita=[]
    for holiday in holidays.Italy(years=[2022]).items():
        #print(holiday[0])
        #print(holiday[1])
        holiday_list.append(holiday)
        holiday_list_pulita.append(holiday[0])
    
    # AGGIUNGO LA FESTA PATRONALE
    logging.debug('Anno corrente = {}'.format(oggi.year))
    holiday_list_pulita.append(datetime.datetime(oggi.year, 6, 24))
    
    if num_giorno==0:
        num=3
        # controllo se venerdì era festivo
        ven = oggi - datetime.timedelta(days = num)
        if ven in holiday_list_pulita:
            num=4
            gio = oggi - datetime.timedelta(days = num)
            if gio in holiday_list_pulita:
                num=5
    elif num_giorno in (5,6):
        num=0
        logging.info('Oggi è {0}, lo script non gira'.format(giorno))
        exit()
    else:
        num=1
        # se oggi è festa
        if oggi in holiday_list_pulita:
            num=0
            logging.info('Oggi è giorno festivo, lo script non gira'.format(giorno))
            exit()
        ieri=oggi - datetime.timedelta(days = num)
        if ieri in holiday_list_pulita:
            # se ieri era lunedì (es. Pasquetta)
            logging.debug('Ieri {}'.format(ieri.strftime('%A')))
            if ieri.weekday()==0:
                num=4 # da ven in poi
            # se ieri era martedì
            elif ieri.weekday()==1:
                num=2
                # verifico altro ieri 
                altroieri=oggi - datetime.timedelta(days = num)
                # se altro ieri era festivo e lunedì (caso di Natale lunedì e S. Stefano Martedì)
                if altroieri in holiday_list_pulita:
                    num=5
            # altrimenti
            else: 
                num=2
                # verifico altro ieri 
                altroieri=oggi - datetime.timedelta(days = num)
                # se altro ieri era festivo e non lunedì (caso di Natale martedì/mercoledì o di due feste vicine)
                if altroieri in holiday_list_pulita:
                    num=3
    
    
    
    
    
    if oggi in holiday_list_pulita:
        logging.debug('Oggi è festa')
    else:
        logging.debug('Oggi non è festa, num={}'.format(num))
        
        
        
if __name__ == "__main__":
    main()