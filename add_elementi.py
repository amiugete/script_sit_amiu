#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2021
# Roberto Marzocchi

'''
Lo script interroga un elenco di piazzole


'''


import os, sys, getopt, re
from tkinter import E, Entry  # ,shutil,glob
import requests
from requests.exceptions import HTTPError




import json


import inspect, os.path




import psycopg2
import sqlite3


currentdir = os.path.dirname(os.path.realpath(__file__))
parentdir = os.path.dirname(currentdir)
sys.path.append(parentdir)

sys.path.append('../')
from credenziali import *

#import requests
import datetime
import time

import ldap

import logging

filename = inspect.getframeinfo(inspect.currentframe()).filename
path = os.path.dirname(os.path.abspath(filename))

#tmpfolder=tempfile.gettempdir() # get the current temporary directory
logfile='{}/log/ldap.log'.format(path)
errorfile='{}/log/ldap_error.log'.format(path)
#if os.path.exists(logfile):
#    os.remove(logfile)

'''logging.basicConfig(
    #handlers=[logging.FileHandler(filename=logfile, encoding='utf-8', mode='w')],
    format='%(asctime)s\t%(levelname)s\t%(message)s',
    #filemode='w', # overwrite or append
    #fileencoding='utf-8',
    #filename=logfile,
    level=logging.DEBUG)
'''




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
     #################################################################
    logger.info('Connessione al db SIT')
    conn = psycopg2.connect(dbname=db,
                        port=port,
                        user=user,
                        password=pwd,
                        host=host)

    curr = conn.cursor()
    curr1 = conn.cursor()
    #conn.autocommit = True
    ###################################################################


    #*************************************************************************************************************
    #   INPUT:
    #  - elenco piazzole con ordine
    #  - tipo di elemento da inserire
    #  - TODo (numero di elementi da inserire)
    #*************************************************************************************************************

    p1= '''
    join (values
    (1	,33037),
(2	,33032),
(3	,33014),
(4	,33036),
(5	,33035),
(6	,32671),
(7	,32675),
(8	,32668),
(9	,32677),
(10	,32683),
(11	,32681),
(12	,32684),
(13	,32674),
(14	,32679),
(15	,32687),
(16	,33079),
(17	,33075),
(18	,33076),
(19	,38012),
(20	,33007),
(21	,38013),
(22	,33065),
(23	,33090),
(24	,33082),
(25	,33068),
(26	,33064),
(27	,33067),
(28	,38564),
(29	,33006),
(30	,33066),
(31	,33070),
(32	,33071),
(33	,33069),
(34	,38106),
(35	,33091),
(36	,33092),
(37	,33089),
(38	,33081),
(39	,33080),
(40	,38015),
(41	,33008),
(42	,33085),
(43	,33055),
(44	,33044),
(45	,33084),
(46	,33030),
(47	,33028),
(48	,33027),
(49	,33041),
(50	,33026),
(51	,33040),
(52	,33088),
(53	,33086),
(54	,33087),
(55	,33045),
(56	,33046),
(57	,33053),
(58	,33051),
(59	,33083),
(60	,38011),
(61	,33073),
(62	,33112),
(63	,33113),
(64	,33129),
(65	,33015),
(66	,33098),
(67	,33097),
(68	,33128),
(69	,33111),
(70	,38997),
(71	,33110),
(72	,38018),
(73	,33054),
(74	,33047),
(75	,33048),
(76	,33052),
(77	,33050),
(78	,33049),
(79	,38582),
(80	,33057),
(81	,33059),
(82	,33056),
(83	,33058),
(84	,33061),
(85	,33062),
(86	,33060),
(87	,33023),
(88	,33022),
(89	,33025),
(90	,33024),
(91	,33031),
(92	,33017),
(93	,33018),
(94	,33020),
(95	,33019),
(96	,33021),
(97	,33033),
(98	,33034),
(99	,33077),
(100	,33109),
(101	,33108),
(102	,33107),
(103	,33105),
(104	,33102),
(105	,33103),
(106	,33101),
(107	,33122),
(108	,33121),
(109	,33118),
(110	,33119),
(111	,33120),
(112	,33131),
(113	,38562),
(114	,33130)
    ) as p1 (id, piazzola)
    '''

    tipo_elemento='RDA360PL'



    #   FINE INPUT
    #*************************************************************************************************************

    query_select0='''SELECT tipo_elemento, tipo_rifiuto FROM elem.tipi_elemento te WHERE descrizione ilike %s'''

    try:
        curr.execute(query_select0, (tipo_elemento,))
        id_tipo=curr.fetchall()
    except Exception as e:
        logger.error(e)

    id_t=0
    for it in id_tipo:
        id_t=it[0]
        tr=it[1]
    if id_t==0:
        logger.error('Controlla il tipo elemento {} che sembra non esistere'.format(tipo_elemento))
        exit()


    curr.close()
    curr = conn.cursor()


    # faccio update solo se già non ci fosse una mail
    query_select='''
    select id, id_piazzola, via, riferimento from
(select id, id_piazzola, via, riferimento, sum(num)
from (  
select
p1.id, e.id_piazzola, v.nome as via,
p2.numero_civico as civ, p2.riferimento, 
case 
when te.nome is null then 'ND'
else te.nome
end nome
, 
count(te.tipo_elemento) as num
from elem.piazzole p2
left join elem.elementi e on p2.id_piazzola = e.id_piazzola 
left join elem.aste a on a.id_asta = p2.id_asta 
left join topo.vie v on v.id_via = a.id_via 
left join elem.tipi_elemento te on te.tipo_elemento = e.tipo_elemento and te.tipo_rifiuto IN (%s) 
{} on p2.id_piazzola = p1.piazzola 
group by
e.id_piazzola, v.nome, p2.numero_civico, p2.riferimento, te.nome, p1.id 
order by
p1.id
) ppp
group by id, id_piazzola, via, riferimento
) pppp where sum = 0
order by id'''.format(p1)
    #logger.debug(query_select)
    try:
        curr.execute(query_select, (tr,))
        piazzole_da_completare=curr.fetchall()
    except Exception as e:
        logger.error(e)

    for pp in piazzole_da_completare:
        #id_piazzola=pp[1]
        logger.info('Aggiunta un bidone {0} su piazzola {1}'.format(tipo_elemento, pp[1]))
        insert_query='''
        INSERT INTO elem.elementi
            (tipo_elemento, 
            id_piazzola, 
            id_asta, 
            x_id_cliente, 
            privato, peso_reale, peso_stimato,
            id_utenza,
            modificato_da,
            data_ultima_modifica, 
            percent_riempimento, 
            freq_stimata, 
            data_inserimento)
            values
            (%s, 
            %s, 
            (select id_asta from elem.piazzole where id_piazzola=%s), 
            '-1'::integer,
            0, 0, 0,
            '-1'::integer,
            '',
            now(), 
            90, 
            3,
            now()
            );'''
        try:
            curr1.execute(insert_query, (id_t,pp[1], pp[1]))
        except Exception as e:
            logger.error(e)

    if len(piazzole_da_completare)==0:
        logger.warning('NON ci sono piazzole da completare')
        exit()

    if input("Sei sicuro di voler continuare ed eseguire il COMMIT (operazione IRREVERSIBILE se non a mano)? [y / other]") == "y":
        logger.info("Eseguo il commit")
        conn.commit()
    else: 
        logger.warning("Sono uscito senza fare nullan")

        
    curr1.close()
    curr.close()
    conn.close()



if __name__ == "__main__":
    main() 