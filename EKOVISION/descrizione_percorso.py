#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2024
# Roberto Marzocchi


import os, sys, re  # ,shutil,glob
import psycopg2


def descrizione_percorso(cod_percorso, data , curr_sit, logger_name):
    '''Funzione per cercare la descrizione di un percorso
        In input vuole:
        - codice_percorso 
        - data_percorso
        - cursore a DB SIT
        - nome del logger
    In output restituisce la descrizione del percorso
    '''
    query='''select descrizione from anagrafe_percorsi.elenco_percorsi ep
    where cod_percorso = %s 
    and to_date(%s,'YYYYMMDD') between data_inizio_validita  and (data_fine_validita- interval '1' day)'''
    
    try:
        curr_sit.execute(query, (cod_percorso, data))
        desc_percorso=curr_sit.fetchall()
    except Exception as e:
        logger_name.error(query)
        logger_name.error(e)
     
       
    if len(desc_percorso)== 1:
        for lp in desc_percorso:
            descrizione=lp[0]
    else:
        descrizione = 'ND'

    return descrizione