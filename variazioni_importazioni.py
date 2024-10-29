#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2021
# Roberto Marzocchi

'''
Lo script verifica le variazioni e manda excel a assterritorio@amiu.genova.it giornalmemte con la sintesi delle stesse 
'''

import os, sys, re  # ,shutil,glob
import inspect, os.path

import xlsxwriter


#import getopt  # per gestire gli input

#import pymssql

import psycopg2

import cx_Oracle

import datetime
import holidays
from workalendar.europe import Italy


from credenziali import *

import report_settimanali_percorsi_ok 


#import requests

import logging
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


from crea_dizionario_da_query import *


import csv


# per mandare file a EKOVISION
import pysftp

#LOG

filename = inspect.getframeinfo(inspect.currentframe()).filename
path     = os.path.dirname(os.path.abspath(filename))



path=os.path.dirname(sys.argv[0]) 
#tmpfolder=tempfile.gettempdir() # get the current temporary directory
logfile='{}/log/variazioni_importazioni.log'.format(path)
errorfile='{}/log/error_variazioni_importazioni.log'.format(path)
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





def cfr_tappe(tappe_sit, tappe_uo, logger):
    ''' Effettua il confronto fra le tappe di SIT e quelle di UO'''
    #logger.info('Richiamo la funzione cfr_tappe')
    check=0
    if len(tappe_sit) == len(tappe_uo) :
        k=0
        while k < len(tappe_sit):
            #logger.debug(tappe_sit[k][0])
            #logger.debug(tappe_uo[k][0])
            
            # nume_seq 0
            if tappe_sit[k][0]!=tappe_uo[k][0]:
                check=1
            # id_via 1
            if tappe_sit[k][1]!=tappe_uo[k][1]:
                check=1    
            # riferimento 3
            if (tappe_uo[k][3] is None and tappe_sit[k][3] is None) or ( (not tappe_uo[k][3] or re.search("^\s*$", tappe_uo[k][3])) and (not tappe_sit[k][3] or re.search("^\s*$", tappe_sit[k][3])) ):
                check1=0
            else:
                if tappe_sit[k][3]!=tappe_uo[k][3]:
                    check=1
                    logger.warning('rif SIT = .{}., rif UO = {}'.format(tappe_sit[k][3], tappe_uo[k][3]))
                    
                
            # frequenza 4
            if tappe_sit[k][4]!=tappe_uo[k][4]:
                check=1   
            # tipo_el 5
            if tappe_sit[k][5]!=tappe_uo[k][5]:
                check=1   
            #id_el 6
            if tappe_sit[k][6]!=tappe_uo[k][6]:
                check=1 
            # ripasso
            if tappe_sit[k][8]!=tappe_uo[k][8]:
                check=1  
            # nota via  7
            if (tappe_uo[k][7] is None and tappe_sit[k][7] is None) or ( (not tappe_uo[k][7] or re.search("^\s*$", tappe_uo[k][7])) and (not tappe_sit[k][7] or re.search("^\s*$", tappe_sit[k][7])) ):
                check1=0
            else:
                if tappe_sit[k][7]!=tappe_uo[k][7] and tappe_uo[k][6] is None: # questo controllo va fatto solo nel caso di spazzamenti (ripasso is null)
                    check=1
                    logger.warning('SIT =  {}, UO = {}'.format(tappe_sit[k][7], tappe_uo[k][7]))
            
            k+=1
    else:
        check=1
    return check



def main():
    # carico i mezzi sul DB PostgreSQL
    logger.info('Connessione al db')
    conn = psycopg2.connect(dbname=db,
                        port=port,
                        user=user,
                        password=pwd,
                        host=host)

    curr = conn.cursor()
    #conn.autocommit = True


    # Mi connetto al DB oracle UO
    cx_Oracle.init_oracle_client(percorso_oracle) # necessario configurare il client oracle correttamente
    #cx_Oracle.init_oracle_client() # necessario configurare il client oracle correttamente
    parametri_con='{}/{}@//{}:{}/{}'.format(user_uo,pwd_uo, host_uo,port_uo,service_uo)
    logger.debug(parametri_con)
    con = cx_Oracle.connect(parametri_con)
    logger.info("Versione ORACLE: {}".format(con.version))
    
    

    oggi=datetime.datetime.today()
    oggi=oggi.replace(hour=0, minute=0, second=0, microsecond=0)
    oggi=datetime.date(oggi.year, oggi.month, oggi.day)
    logging.debug('Oggi {}'.format(oggi))
    
    
    num_giorno=datetime.datetime.today().weekday()
    giorno=datetime.datetime.today().strftime('%A')
    giorno_file=datetime.datetime.today().strftime('%Y%m%d')
    oggi1=datetime.datetime.today().strftime('%d/%m/%Y')
    logging.debug('Il giorno della settimana è {} o meglio {}'.format(num_giorno, giorno))
    
    
    
    holiday_list = []
    holiday_list_pulita=[]
    for holiday in holidays.Italy(years=[(oggi.year -1), (oggi.year)]).items():
        #print(holiday[0])
        #print(holiday[1])
        holiday_list.append(holiday)
        holiday_list_pulita.append(holiday[0])
    
    
    # AGGIUNGO LA FESTA PATRONALE
    logging.debug('Anno corrente = {}'.format(oggi.year))
    fp = datetime.datetime(oggi.year, 6, 24)
    festa_patronale=datetime.date(fp.year, fp.month, fp.day)
    holiday_list_pulita.append(festa_patronale)
    
    # aggiungo giorno in cui non sono passate le variazioni
    fp = datetime.datetime(oggi.year, 2, 19)
    giorno_variazioni_saltate=datetime.date(fp.year, fp.month, fp.day)
    holiday_list_pulita.append(giorno_variazioni_saltate)
    
    if num_giorno==0:
        num=3
        # controllo se venerdì era festivo
        ven = oggi - datetime.timedelta(days = num)
        ven=datetime.date(ven.year, ven.month, ven.day)
        if ven in holiday_list_pulita:
            num=4
            gio = oggi - datetime.timedelta(days = num)
            gio=datetime.date(gio.year, gio.month, gio.day)
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
        ieri=datetime.date(ieri.year, ieri.month, ieri.day)
        #logging.debug('Ieri = {}'.format(ieri))
        #logging.debug(holiday_list_pulita)
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
                altroieri=datetime.date(altroieri.year, altroieri.month, altroieri.day)
                # se altro ieri era festivo e lunedì (caso di Natale lunedì e S. Stefano Martedì)
                if altroieri in holiday_list_pulita:
                    num=5
            # altrimenti
            else: 
                num=2
                # verifico altro ieri 
                altroieri=oggi - datetime.timedelta(days = num)
                altroieri=datetime.date(altroieri.year, altroieri.month, altroieri.day)
                # se altro ieri era festivo e non lunedì (caso di Natale martedì/mercoledì o di due feste vicine)
                if altroieri in holiday_list_pulita:
                    num=3
                    
    
    logging.debug('num = {}'.format(num))
                    
                    
                    
    
    #check_error=0 # va messo sotto 
    
    
    '''******************************************************************************************************
    NON SONO COMPRESI I PERCORSI STAGIONALI per cui vanno re-importate le variazioni in fase di attivazione 
    ********************************************************************************************************'''
    
    '''IMPORTAZIONE MASSIVA TUTTI PERCORSI APP SPAZZAMENTOselect distinct p.cod_percorso , p.descrizione, s.descrizione as servizio, u.descrizione  as ut,
        p.id_percorso
        from elem.percorsi p 
        inner join elem.percorsi_ut pu 
        on pu.cod_percorso =p.cod_percorso 
        inner join elem.servizi s 
        on s.id_servizio =p.id_servizio
        inner join topo.ut u 
        on u.id_ut = pu.id_ut
       where s.id_servizio in (select distinct id_servizio_sit 
        from anagrafe_percorsi.anagrafe_tipo at2 
        where gestito_app_spazzamento = 'S' and id_servizio_sit is not null)
        and p.id_categoria_uso in (3,6)
        UNION'''
    
    query='''select distinct p.cod_percorso , p.descrizione, s.descrizione as servizio, u.descrizione  as ut,
        p.id_percorso
        from util.sys_history h
        inner join elem.percorsi p 
        on h.id_percorso = p.id_percorso 
        inner join elem.percorsi_ut pu 
        on pu.cod_percorso =p.cod_percorso 
        inner join elem.servizi s 
        on s.id_servizio =p.id_servizio
        inner join topo.ut u 
        on u.id_ut = pu.id_ut 
        where h.datetime > (current_date - INTEGER '{0}') 
        and h.datetime <= current_date::date 
        and (
        (h."type" IN ('PERCORSO') 
        and h.action IN ('UPDATE_ELEM', 'UPDATE')
        ) or 
        (h."type" IN ('ASTA PERCORSO') 
        and h.action IN ('INSERT', 'UPDATE', 'DELETE')
        )
        )
        and pu.responsabile = 'S'
        and p.id_categoria_uso in (3)
        and (p.data_dismissione is null or p.data_dismissione > current_date )
        and p.data_attivazione <= current_date::date
        UNION 
        select p2.cod_percorso , p2.descrizione, s2.descrizione as servizio, u2.descrizione  as ut, 
        p2.id_percorso
        from elem.percorsi p2 
        join elem.servizi s2 on s2.id_servizio = p2.id_servizio 
        inner join elem.percorsi_ut pu2 
        on pu2.cod_percorso =p2.cod_percorso
        inner join topo.ut u2 
        on u2.id_ut = pu2.id_ut 
        where pu2.responsabile = 'S'
        and p2.id_categoria_uso in (3)
        and p2.data_attivazione > (current_date - INTEGER '{0}')
        and p2.data_attivazione <= current_date::date
        UNION
        select distinct p3.cod_percorso , p3.descrizione, s3.descrizione as servizio, u3.descrizione  as ut,
        p3.id_percorso
        from elem.elementi e
        join (
        select datetime, description, id_piazzola, split_part(replace(description, 'Elementi tipo ', ''), ' ',1) as tipo_elemento 
        from util.sys_history sh 
        where type='PIAZZOLA_ELEM' and action = 'UPDATE' and description ilike 'Elementi tipo%' 
        and sh.datetime > (current_date - INTEGER '{0}')
        and sh.datetime <= current_date::date
        and id_percorso is null 
        ) b on b.id_piazzola=e.id_piazzola and b.tipo_elemento::int = e.tipo_elemento and date_trunc('second', e.data_inserimento) != date_trunc('second', b.datetime)  
        join elem.elementi_aste_percorso eap on eap.id_elemento = e.id_elemento 
        join elem.aste_percorso ap on eap.id_asta_percorso = ap.id_asta_percorso 
        join elem.percorsi p3 on p3.id_percorso = ap.id_percorso 
        join elem.servizi s3 on s3.id_servizio = p3.id_servizio 
        inner join elem.percorsi_ut pu3 
        on pu3.cod_percorso =p3.cod_percorso
        inner join topo.ut u3 
        on u3.id_ut = pu3.id_ut 
        where pu3.responsabile = 'S'
        and p3.id_categoria_uso in (3)
        and (p3.data_dismissione is null or p3.data_dismissione > current_date::date )
        and p3.data_attivazione <= current_date::date
        order by ut, servizio
        '''.format(num)
    


    try:
        curr.execute(query)
        lista_variazioni=curr.fetchall()
    except Exception as e:
        logger.error(e)


    #inizializzo gli array
    cod_percorso=[]
    descrizione=[]
    servizio=[]
    ut=[]
    stato_importazione=[]
    invio_mail=[]

           
    for vv in lista_variazioni:
        check_error=0
        logger.debug(vv[0])
        cod_percorso.append(vv[0])
        descrizione.append(vv[1])
        servizio.append(vv[2])
        ut.append(vv[3])
        
        ########################################################################################################
        # cerco se il percorso esiste per gestire la nuova tabella anagrafe_percorsi.date_modifica_itinerari
        curr1 = conn.cursor()
        sel_date = '''select * from anagrafe_percorsi.date_modifica_itinerari where cod_percorso = %s'''  
        try:
            curr1.execute(sel_date, (vv[0],))
            lista_date=curr1.fetchall()
        except Exception as e:
            logger.error(e)
        
        
        if len(lista_date)==0:
            # insert
            curr2 = conn.cursor()
            insert_q='''insert into anagrafe_percorsi.date_modifica_itinerari (cod_percorso, data_ultima_modifica)
            values (%s, to_date(%s, 'YYYYMMDD'))'''
            curr2.execute(insert_q, (vv[0],giorno_file))
            conn.commit()
            curr2.close()
        else:
            #UPDATE
            for ld in lista_date:
                logger.debug(ld[0])
                logger.debug(ld[1])
            curr2 = conn.cursor()
            insert_q='''UPDATE anagrafe_percorsi.date_modifica_itinerari 
            set data_ultima_modifica= to_date(%s, 'YYYYMMDD')
            where cod_percorso = %s'''
            curr2.execute(insert_q, (giorno_file, vv[0]))
            conn.commit()
            curr2.close()
            
        curr1.close() 
        
        
        
        ########################################################################################################
        # CAMBIO DATA ATTIVAZIONE SU SIT
        curr1 = conn.cursor()       
        insert_query='''
            update elem.percorsi set data_attivazione = now()::date
            where data_attivazione < now() and 
            id_percorso=%s
        '''
        
        try:
            curr1.execute(insert_query, (int(vv[4]),))
            #lista_variazioni=curr.fetchall()
        except Exception as e:
            logger.warning('Codice percorso = {}'.format(vv[0]))
            logger.error(insert_query)
            logger.error(e)                                            

        curr1.close()
        conn.commit()
        
        
        
        # update delle NOTE elementi_aste_percorsi per i lavaggi con Botticella
        curr1 = conn.cursor()
        update_note='''
            update elem.aste_percorso ap0
            set nota = (select string_agg(e.riferimento, ',')
                from elem.aste_percorso ap  
                join elem.elementi_aste_percorso eap on eap.id_asta_percorso = ap.id_asta_percorso 
                join elem.elementi e on eap.id_elemento = e.id_elemento 
                where ap.id_asta_percorso = ap0.id_asta_percorso and ap.num_seq= ap0.num_seq
                group by ap.id_asta_percorso, ap.nota
            )
            where id_percorso in 
                (select id_percorso from elem.percorsi p where id_servizio =33 and id_categoria_uso in (3,6))
        '''
        
        try:
            curr1.execute(update_note)
            #lista_variazioni=curr.fetchall()
        except Exception as e:
            logger.error(update_note)
            logger.error(e)                                            

        curr1.close()
        conn.commit()
        



        # 1 - verifico se c'è una testata attiva su UO
        cur = con.cursor()
        check1=0
        query_o1='''SELECT count(*) FROM ANAGR_SER_PER_UO aspu 
        WHERE ID_PERCORSO = :cod_perc
        AND DTA_ATTIVAZIONE <= TO_DATE (:data1, 'DD/MM/YYYY') 
        AND DTA_DISATTIVAZIONE > TO_DATE (:data2, 'DD/MM/YYYY') '''

        try:
            cur.execute(query_o1, (vv[0], oggi1, oggi1))
            cc_pp=cur.fetchall()
        except Exception as e:
            logger.error(query_o1)
            logger.error(e)                                            


        for c_p in cc_pp:
            logger.debug(c_p[0])
            if(c_p[0])>0:
                check1=1
            else:
                stato_importazione.append('ERRORE: Non ci sono testate su UO')
        logger.debug('Check1={}'.format(check1))        
        cur.close()
        
        # recupero le mail cui inviare il report
        if (check1==1): 
            cur = con.cursor()
            query_uo='''SELECT aspu.ID_UO, au.MAIL FROM ANAGR_SER_PER_UO aspu 
            JOIN ANAGR_UO au ON au.ID_UO = aspu.ID_UO  
            WHERE aspu.ID_PERCORSO = :cod_perc
            AND aspu.DTA_ATTIVAZIONE <= TO_DATE (:data1, 'DD/MM/YYYY') 
            AND aspu.DTA_DISATTIVAZIONE > TO_DATE (:data2, 'DD/MM/YYYY') '''

            try:
                cur.execute(query_uo, (vv[0], oggi1, oggi1))
                uu_oo=cur.fetchall()
            except Exception as e:
                logger.error(query_uo)
                logger.error(e)                                            

            invio_mail_tmp=''
            for u_o in uu_oo:
                logger.debug(u_o[1])
                # le mail sono aggiornate a partire dall'HUB di GAVA 
                if invio_mail_tmp!='':
                    invio_mail_tmp='{}, {}'.format(invio_mail_tmp,u_o[1])
                else: 
                    invio_mail_tmp='{}'.format(u_o[1])
            
            invio_mail.append(invio_mail_tmp)
                
                        
            cur.close()
        else:
            invio_mail.append('')    
        
        # Se ho superato primo check verifico che il percorso non sia già importato
        check2=0
        if (check1==1):            
            cur = con.cursor()
            cod_perc=vv[0]
            data1=oggi1
            query_o2='''SELECT ID_PERCORSO, max(DATA_PREVISTA) AS DATA_PREVISTA
            FROM CONS_PERCORSI_VIE_TAPPE cpvt  
            WHERE ID_PERCORSO = :cod_perc AND TRUNC(DATA_PREVISTA) >= TO_DATE (:data1, 'DD/MM/YYYY')
            GROUP BY ID_PERCORSO'''

            try:
                cur.execute(query_o2, (cod_perc,data1))
                #logger.debug(query_o2,(cod_perc,data1))
                pp_dd=cur.fetchall()
            except Exception as e:
                logger.error(query_o2, cod_perc,data1)
                logger.error(e)                                            

            if len(pp_dd)>0:
                check2=0
                stato_importazione.append('WARNING: Percorso già importato con data odierna o successiva')
            else:
                check2=1
            
            for p_d in pp_dd:
                logger.warning(p_d[0])
                logger.warning(p_d[1])

            cur.close()
        
        logger.debug('Ora procedo con la verifica del tipo di percorso. Check1={}, Check2={}'.format(check1, check2))
        
        check3=0
        # procedeo con la verifica del percorso
        if (check1==1 and check2 == 1):
            #logger.debug('Entro qua')
            cur = con.cursor()
            risp='?'
            try:
                ret=cur.callproc('UNIOPE.CONTROLLAPERCORSO',
                         [vv[0],oggi1,risp])
            except Exception as e:
                #logger.error(query_o3)
                logger.error(e)
            
            
            
            """query_o3='''CALL UNIOPE.CONTROLLAPERCORSO(:cod_perc,:data1,:nret);'''

            try:
                cur.execute(query_o3, (vv[0],oggi1, '?'))
                risp=cur.fetchone()
            except Exception as e:
                logger.error(query_o3)
                logger.error(e)      
            """
            try:    
                controllo_percorso=int(ret[2])
            except Exception as e:
                #logger.error(query_o3)
                logger.error(e)
                logger.debug(ret)
            
            cur.close()
                
            
            if controllo_percorso == -2:
                stato_importazione.append('ERRORE: Percorso già consuntivato con delle causali')
            elif (controllo_percorso == -1 or controllo_percorso==1):
                check3=1
            else:
                stato_importazione.append('''ERRORE: CALL UNIOPE.CONTROLLAPERCORSO({},{},?) restituisce dei dati anomali
                                        , ripulire la consuntivazione e provare a re-importare'''.format(vv[0],oggi1))
         
        # se tutto OK procedo con l'importazione
        if (check1==1 and check2 == 1 and check3 == 1): 
            
            
            # ANDRA' POI FATTA SU SIT UNA PULIZIA DI SERVIZI 
            '''select distinct ap.id_asta_percorso, num_seq, tipo, eap.frequenza, ap.id_asta  from elem.aste_percorso ap 
            left join elem.elementi_aste_percorso eap on eap.id_asta_percorso = ap.id_asta_percorso 
            where ap.id_percorso = 200296 

            select * from elem.elementi_aste_percorso eap where id_asta_percorso in 
            (select id_asta_percorso  from elem.aste_percorso ap where id_percorso = 200296 )'''
            
            # cerco se raccolta o spazzamento o altro e salvo il risultato nella variabile tipo_percorso
            cur = con.cursor()
            query_tipo= ''' SELECT GETTIPOPERCORSO(:cod_perc, TO_DATE (:data1, 'DD/MM/YYYY')) FROM DUAL'''
            try:
                cur.execute(query_tipo, (cod_perc,data1))
                tt_pp=cur.fetchall()
            except Exception as e:
                logger.error(query_tipo)
                logger.error(e)                                            


            
            for t_p in tt_pp:
                tipo_percorso=t_p[0]
            
            cur.close()
            
            
            # importazione macro tappe
            
            # cerco la max macro tappa 
            cur = con.cursor()
            query_id_t= '''SELECT max(ID_TAPPA) FROM CONS_PERCORSI_VIE_TAPPE'''
            try:
                cur.execute(query_id_t)
                ii_tt=cur.fetchall()
            except Exception as e:
                logger.error(query_id_t)
                logger.error(e)                                            


            
            for i_t in ii_tt:
                max_id_macro_tappa=i_t[0]
            
            cur.close()
            
            
            # cerco la max micro tappa - NON SERVE
            """cur = con.cursor()
            query_id_t= '''SELECT max(ID_MICRO_TAPPA) FROM CONS_MICRO_TAPPA'''
            try:
                cur.execute(query_id_t)
                ii_tt=cur.fetchall()
            except Exception as e:
                logger.error(query_id_t)
                logger.error(e)                                            


            
            for i_t in ii_tt:
                max_id_micro_tappa=i_t[0]
            
            cur.close()
            """
            
            
            
            
            
            if tipo_percorso=='R':
                """
                devo inserire:
                 - macro tappe
                 - cons_vie_tappe
                 - micro tappe
                 
                 Tutto a partire dalla etl.v_tappe di SIT
                
                """ 
            
                # PRIMA VERIFICO SE CI SIANO DIFFERENZE CHE GIUSTIFICHINO IMPORTAZIONE
                curr1 = conn.cursor()
                """"sel_sit='''select vt.num_seq, id_via::int, coalesce(numero_civico,' ') as numero_civico , 
                coalesce(riferimento,' ') as riferimento, fo.freq_binaria as frequenza,vt.tipo_elemento, vt.id_elemento::int,
                coalesce(vt.nota_asta, ' ') as nota_asta
                from etl.v_tappe vt 
                join etl.frequenze_ok fo on fo.cod_frequenza = vt.frequenza_elemento::int 
                where id_percorso = %s  
                order by num_seq , numero_civico, riferimento, id_elemento '''
                """
                
                sel_sit='''select vt.num_seq, id_via::int, coalesce(numero_civico,' ') as numero_civico , 
                coalesce(riferimento,' ') as riferimento, fo.freq_binaria as frequenza,vt.tipo_elemento, vt.id_elemento::int,
                coalesce(riferimento, ' ') as nota_asta,  coalesce (ripasso, 0) as ripasso
                from etl.v_tappe vt 
                join etl.frequenze_ok fo on fo.cod_frequenza = vt.frequenza_elemento::int 
                where id_percorso = %s  
                order by num_seq , numero_civico, riferimento, id_elemento '''
                
                try:
                    curr1.execute(sel_sit, (vv[4],))
                    #logger.debug(query_sit1, max_id_macro_tappa, vv[4] )
                    #curr1.rowfactory = makeDictFactory(curr1)
                    tappe_sit=curr1.fetchall()
                except Exception as e:
                    logger.error(sel_sit, vv[4] )
                    logger.error(e)
                
                
                cur1 = con.cursor()
                sel_uo='''SELECT VTP.CRONOLOGIA NUM_SEQ,VTP.ID_VIA, NVL(VTP.NUM_CIVICO,' ') as  NUMERO_CIVICO,
                NVL(VTP.RIFERIMENTO, ' ') as RIFERIMENTO,
                VTP.FREQELEM, VTP.TIPO_ELEMENTO, TO_NUMBER(VTP.ID_ELEMENTO) AS ID_ELEM_INT,
                 NVL(VTP.NOTA_VIA, ' ') as NOTA_VIA, COALESCE(TO_NUMBER(RIPASSO),0) AS RIPASSO
                FROM V_TAPPE_ELEMENTI_PERCORSI VTP
                inner join (select MAX(CPVT.DATA_PREVISTA) data_prevista, CPVT.ID_PERCORSO
                 from CONS_PERCORSI_VIE_TAPPE CPVT
                where CPVT.DATA_PREVISTA<=TO_DATE(:t1,'DD/MM/YYYY') 
                group by CPVT.ID_PERCORSO) PVT
                on PVT.ID_PERCORSO=VTP.ID_PERCORSO 
               	and vtp.data_prevista = pvt.data_prevista
                where VTP.ID_PERCORSO=:t2
                ORDER BY VTP.CRONOLOGIA, NUMERO_CIVICO, RIFERIMENTO, ID_ELEM_INT
                ''' 
                try:
                    cur1.execute(sel_uo, (oggi1, vv[0]))
                    #cur1.rowfactory = makeDictFactory(cur1)
                    tappe_uo=cur1.fetchall()
                except Exception as e:
                    logger.error(sel_uo, oggi1, vv[0] )
                    logger.error(e)
                
                curr1.close()  
                cur1.close()      
                logger.debug('Trovate {} tappe su SIT per il percorso {}'.format(len(tappe_sit),vv[0]))
                logger.debug('Trovate {} tappe su UO per il percorso {}'.format(len(tappe_uo),vv[0]))
                #logger.debug(tappe_sit[:][1])
                #logger.debug(tappe_uo[:][1])
                
                
                
                to_import = 1
                
                if len(tappe_sit) == 0: 
                    logger.info('Percorso {} non ha più tappe su SIT.'.format(vv[0]))
                    stato_importazione.append('WARNING - Percorso senza tappe su SIT. Verificare e probabilmente disattivare')         
                    to_import=0
                elif len (tappe_uo) ==0:
                    logger.info( 'Su UO non ci sono ancora tappe. Ora le importo')
                elif len(tappe_uo) > 0 and cfr_tappe(tappe_sit, tappe_uo, logger)==0 :
                    logger.info('Percorso {} già importato con data antecedente. Non ci sono state modifiche sostanziali.'.format(vv[0]))
                    stato_importazione.append('Percorso già importato con data antecedente. Non ci sono state modifiche sostanziali.')
                    to_import=0
                
                if to_import==1:
                           
                    # procedo con importazione
                    curr1 = conn.cursor()
                    curr2 = conn.cursor()
                    
                    query_sit1='''select  (row_number() OVER 
                    (ORDER BY vt.num_seq, case when numero_civico='' then null else numero_civico end nulls last, riferimento)+%s),
                            riferimento,
                            0 as qta_tot_spazzamento, fo.freq_binaria as frequenza, 
                            /*case 
                            when COALESCE (vt.ripasso, 0) > 0 then 1
                            else  COALESCE (vt.ripasso, 0)
                            end ripasso,*/
                            COALESCE (vt.ripasso, 0) as ripasso,
                            id_piazzola, id_asta,  lung_trattamento, 
                            vt.cod_percorso, vt.id_via, vt.num_seq as cronologia, 
                            now() as dta_import, (now()::date)::timestamp as data_prevista, numero_civico, tipo_elemento
                            ,nota_asta, 
                            case 
                                when id_piazzola is null then id_elemento 
                                else null
                            end as id_elemento_no_piazzola
                            from etl.v_tappe vt 
                            join etl.frequenze_ok fo on fo.cod_frequenza = vt.frequenza_elemento::int 
                            where id_percorso = %s 
                            group by vt.num_seq, riferimento,
                            id_piazzola, id_asta, fo.freq_binaria, 
                            ripasso, lung_trattamento, 
                            vt.cod_percorso, vt.id_via, vt.num_seq, vt.numero_civico, tipo_elemento, nota_asta,
                            case 
                                when id_piazzola is null then id_elemento 
                                else null
                            end
                            order by num_seq, case when numero_civico='' then null else numero_civico end nulls last, riferimento  '''
                    
                    
                    
                    try:
                        curr1.execute(query_sit1, (max_id_macro_tappa, vv[4]))
                        #logger.debug(query_sit1, max_id_macro_tappa, vv[4] )
                        sit1=curr1.fetchall()
                    except Exception as e:
                        logger.error(query_sit1, max_id_macro_tappa, vv[4] )
                        logger.error(e)
                    
                    
                        
                    macro_tappe=[]    
                    cur = con.cursor()
                    cur1 = con.cursor()
                    cur2 = con.cursor()
                    for tappa in sit1:
                        
                        
                        query_insert0='''INSERT INTO UNIOPE.CONS_MACRO_TAPPA
                        (ID_MACRO_TAPPA, RIFERIMENTO, QTA_TOT_SPAZZAMENTO, FREQUENZA, RIPASSO, ID_PIAZZOLA, ID_ASTA, LUNG_TRATTAMENTO, NOTA_VIA)
                        VALUES(:t1, :t2, :t3, :t4, :t5, :t6, :t7, :t8, :t9)'''
        
        
                        try:
                            cur.execute(query_insert0, (tappa[0], tappa[1], tappa[2], tappa[3], tappa[4], tappa[5], tappa[6], tappa[7], tappa[15]))
                            #macro_tappe.append(tappa[0])
                        except Exception as e:
                            check_error=1
                            logger.error(tappa)
                            logger.error(query_insert0)
                            logger.error(e)
                            check_error=1   
        
        
                        # dopo aver inserito le macro tappe ora mi concentro sulle CONS_PERCORSI_VIE_TAPPE
                        curr1 = conn.cursor()
                    
                        query_sit1=''''''
                        
                        
                        
                        # CONS_PERCORSI_VIE_TAPPE
                        query_insert1='''INSERT INTO UNIOPE.CONS_PERCORSI_VIE_TAPPE
                    (ID_PERCORSO, ID_VIA, ID_TAPPA, CRONOLOGIA, DTA_IMPORT, DATA_PREVISTA)
                    VALUES(:t1, :t2, :t3, :t4, :t5, :t6)
                    '''
                    
                    
                        try:
                            cur.execute(query_insert1, (tappa[8], tappa[9], tappa[0], tappa[10], tappa[11], tappa[12]))
                            #macro_tappe.append(tappa[2])
                        except Exception as e:
                            check_error=1
                            #logger.error(tappa)
                            logger.error(query_insert1)
                            logger.error('1:{}, 2:{}, 3:{}, 4:{}, 5:{}, 6:{}'.format(tappa[8], tappa[9], tappa[0], tappa[10], tappa[11], tappa[12]))
                            logger.error(e)
                            check_error=1                                            



                        # ORA DEVO POPOLARE MICROTAPPE E CONS_ELEMENTI
                        query_insert3='''INSERT INTO UNIOPE.CONS_MICRO_TAPPA
                        (ID_MICRO_TAPPA, ID_MACRO_TAPPA, FREQUENZA, RIPASSO, NUM_CIVICO, POSIZIONE, ID_ELEMENTO, ID_PIAZZOLA)
                        VALUES(
                            (SELECT max(ID_MICRO_TAPPA)+1 FROM CONS_MICRO_TAPPA),
                            :t1, :t2, :t3, :t4, :t5, :t6, :t7)'''

                        query_insert3_nopiazzola='''INSERT INTO UNIOPE.CONS_MICRO_TAPPA
                        (ID_MICRO_TAPPA, ID_MACRO_TAPPA, FREQUENZA, RIPASSO, NUM_CIVICO, POSIZIONE, ID_ELEMENTO, ID_PIAZZOLA)
                        VALUES(
                            (SELECT max(ID_MICRO_TAPPA)+1 FROM CONS_MICRO_TAPPA),
                            :t1, :t2, :t3, :t4, :t5, :t6, NULL)'''

                        
                        if tappa[5] is None:
                            query_sit2='''select fo.freq_binaria as frequenza, 
                            /*case 
                                when COALESCE (vt.ripasso, 0) > 0 then 1
                                else  COALESCE (vt.ripasso, 0)
                                end ripasso, */
                            COALESCE (vt.ripasso, 0) as ripasso,
                            numero_civico, riferimento, 
                            id_elemento, id_piazzola, tipo_elemento, num_seq
                            from etl.v_tappe vt 
                            join etl.frequenze_ok fo on fo.cod_frequenza = vt.frequenza_elemento::int 
                            where id_percorso = %s and tipo_elemento = %s and id_elemento=%s
                            order by num_seq, case when numero_civico='' then null else numero_civico end nulls last, id_elemento'''
                            
                            try:
                                curr2.execute(query_sit2, (vv[4], tappa[14], tappa[16]))
                                sit2=curr2.fetchall()
                            except Exception as e:
                                logger.error(query_sit2, vv[4], tappa[14], tappa[10] )
                                logger.error(e)
                        else:
                            query_sit2='''select fo.freq_binaria as frequenza, 
                            /*case 
                                when COALESCE (vt.ripasso, 0) > 0 then 1
                                else  COALESCE (vt.ripasso, 0)
                                end ripasso, */
                            COALESCE (vt.ripasso, 0) as ripasso,
                            numero_civico, riferimento, 
                            id_elemento, id_piazzola, tipo_elemento, num_seq
                            from etl.v_tappe vt 
                            join etl.frequenze_ok fo on fo.cod_frequenza = vt.frequenza_elemento::int 
                            where id_percorso = %s and id_piazzola=%s and tipo_elemento = %s and num_seq= %s
                            order by num_seq, case when numero_civico='' then null else numero_civico end nulls last, id_elemento'''
                            
                            try:
                                curr2.execute(query_sit2, (vv[4], tappa[5], tappa[14], tappa[10]))
                                sit2=curr2.fetchall()
                            except Exception as e:
                                logger.error(query_sit2, vv[4], tappa[5], tappa[14], tappa[10] )
                                logger.error(e)
                        
                        
                        for elementi in sit2:
                            
                            # per prima cosa cerco se esiste già un elemento e faccio update o insert del tipo elemento
                            query_sel_el='''SELECT TIPO_ELEMENTO, ID_ELEMENTO FROM UNIOPE.CONS_ELEMENTI WHERE ID_ELEMENTO = :t1
                            '''
                            try:
                                cur1.execute(query_sel_el, (str(elementi[4]),))
                                ee=cur1.fetchall()
                            except Exception as e:
                                check_error=1
                                logger.error(str(elementi[4]))
                                logger.error(query_sel_el)
                                logger.error(e)
                                check_error=1
                            
                            if len(ee)==1:
                                # update
                                query_update='''UPDATE UNIOPE.CONS_ELEMENTI SET TIPO_ELEMENTO=:t1
                                WHERE ID_ELEMENTO=:t2'''
                                try:
                                    cur.execute(query_update, (elementi[6], str(elementi[4])))
                                    #macro_tappe.append(tappa[2])
                                except Exception as e:
                                    check_error=1
                                    logger.error(elementi)
                                    logger.error(query_insert4)
                                    logger.error(e)
                                    check_error=1
                            else:
                                #insert
                                query_insert4='''INSERT INTO UNIOPE.CONS_ELEMENTI
                                (ID_ELEMENTO, TIPO_ELEMENTO)
                                VALUES (:t1, :t2)'''
                                try:
                                    cur.execute(query_insert4, (str(elementi[4]), elementi[6]))
                                    #macro_tappe.append(tappa[2])
                                except Exception as e:
                                    check_error=1
                                    logger.error(elementi)
                                    logger.error(query_insert4)
                                    logger.error(e)
                                    check_error=1

                            # dopo aver inserito gli elementi ora posso inserire le microtappe
                            
                            if elementi[5] is None:
                                #query_insert3_nopiazzola
                                try:
                                    cur2.execute(query_insert3_nopiazzola, (tappa[0], elementi[0], elementi[1], elementi[2], elementi[3], str(elementi[4])))
                                    #macro_tappe.append(tappa[2])
                                except Exception as e:
                                    check_error=1
                                    logger.error(tappa[0])
                                    logger.error('elementi {}'.format(elementi))
                                    logger.error(query_insert3)
                                    logger.error(e)
                                    check_error=1
                            else:
                                try:
                                    cur2.execute(query_insert3, (tappa[0], elementi[0], elementi[1], elementi[2], elementi[3], str(elementi[4]), elementi[5]))
                                    #macro_tappe.append(tappa[2])
                                except Exception as e:
                                    check_error=1
                                    logger.error(tappa[0])
                                    logger.error('elementi {}'.format(elementi))
                                    logger.error(query_insert3)
                                    logger.error(e)
                                    check_error=1
                        
                    # chiudo connessione oracle
                    cur.close()
                    cur1.close()
                    cur2.close()
                    con.commit()
                    curr1.close()
                    curr2.close()
                    
                    

                    #exit()
            elif tipo_percorso=='S':
                
                # anche in questo caso devo vedere se ci sono differenze
                
                # PRIMA VERIFICO SE CI SIANO DIFFERENZE CHE GIUSTIFICHINO IMPORTAZIONE
                curr1 = conn.cursor()
                sel_sit='''select vt.num_seq, id_via::int, coalesce(numero_civico,' ') as numero_civico , 
                coalesce(riferimento,' ') as riferimento, fo.freq_binaria as frequenza,vt.tipo_elemento, vt.id_elemento::int,
                coalesce(vt.nota_asta, ' ') as nota_asta, coalesce(vt.ripasso, 0) as ripasso
                from etl.v_tappe vt 
                join etl.frequenze_ok fo on fo.cod_frequenza = vt.frequenza_asta::int 
                where id_percorso = %s  
                order by num_seq , case when numero_civico='' then null else numero_civico end nulls last, id_elemento '''
                try:
                    curr1.execute(sel_sit, (vv[4],))
                    #logger.debug(query_sit1, max_id_macro_tappa, vv[4] )
                    #curr1.rowfactory = makeDictFactory(curr1)
                    tappe_sit=curr1.fetchall()
                except Exception as e:
                    logger.error(sel_sit, vv[4] )
                    logger.error(e)
                
                
                cur1 = con.cursor()
                sel_uo='''SELECT VTP.CRONOLOGIA NUM_SEQ, VTP.ID_VIA, NVL(VTP.NUM_CIVICO,' ') as  NUMERO_CIVICO,
                NVL(VTP.RIFERIMENTO, ' ') as RIFERIMENTO,
                VTP.FREQELEM, VTP.TIPO_ELEMENTO, TO_NUMBER(VTP.ID_ELEMENTO) AS ID_ELEM_INT,
                CASE 
	                 WHEN VTP.NOTA_VIA IS NULL AND VTP.RIFERIMENTO IS NOT NULL THEN VTP.RIFERIMENTO
                 	 ELSE NVL(VTP.NOTA_VIA, ' ')
                 END
                  as NOTA_VIA, COALESCE(TO_NUMBER(RIPASSO),0) AS RIPASSO
                FROM V_TAPPE_ELEMENTI_PERCORSI VTP
                inner join (select MAX(CPVT.DATA_PREVISTA) data_prevista, CPVT.ID_PERCORSO
                 from CONS_PERCORSI_VIE_TAPPE CPVT
                where CPVT.DATA_PREVISTA<=TO_DATE(:t1,'DD/MM/YYYY') 
                group by CPVT.ID_PERCORSO) PVT
                on PVT.ID_PERCORSO=VTP.ID_PERCORSO 
               	and vtp.data_prevista = pvt.data_prevista
                where VTP.ID_PERCORSO=:t2
                ORDER BY VTP.CRONOLOGIA, NUMERO_CIVICO, ID_ELEM_INT
                ''' 
                try:
                    cur1.execute(sel_uo, (oggi1, vv[0]))
                    #cur1.rowfactory = makeDictFactory(cur1)
                    tappe_uo=cur1.fetchall()
                except Exception as e:
                    logger.error(sel_uo, oggi1, vv[0] )
                    logger.error(e)
                
                curr1.close()  
                cur1.close()      
                logger.debug('Trovate {} tappe su SIT per il percorso {}'.format(len(tappe_sit),vv[0]))
                logger.debug('Trovate {} tappe su UO per il percorso {}'.format(len(tappe_uo),vv[0]))
                #logger.debug(tappe_sit[:][1])
                #logger.debug(tappe_uo[:][1])
                
                to_import =1
                
                if len(tappe_sit) == 0: 
                    logger.info('Percorso {} non ha più tappe su SIT.'.format(vv[0]))
                    stato_importazione.append('WARNING - Percorso senza tappe su SIT. Verificare e probabilmente disattivare')         
                    to_import=0
                elif len (tappe_uo) ==0:
                    logger.info( 'Su UO non ci sono ancora tappe. Ora le importo')
                elif len(tappe_uo) > 0 and cfr_tappe(tappe_sit, tappe_uo, logger)==0 :
                    logger.info('Percorso {} già importato con data antecedente. Non ci sono state modifiche sostanziali.'.format(vv[0]))
                    stato_importazione.append('Percorso già importato con data  antecedente. Non ci sono state modifiche sostanziali.')
                    to_import=0
                
                if to_import==1:
                
                    curr1 = conn.cursor()
                    curr2 = conn.cursor()
                    
                    query_sit1='''select  (row_number() OVER (ORDER BY vt.num_seq)+%s), vt.nota_asta,
                            vt.mq_trattati, fo.freq_binaria as frequenza, COALESCE (vt.ripasso, 0) as ripasso,
                            id_asta,  lung_trattamento, 
                            vt.cod_percorso, vt.id_via, vt.num_seq as cronologia, 
                            now() as dta_import, (now()::date)::timestamp as data_prevista
                            from etl.v_tappe vt 
                            join etl.frequenze_ok fo on fo.cod_frequenza = vt.frequenza_asta::int 
                            where id_percorso = %s 
                            group by  vt.num_seq, vt.nota_asta, vt.mq_trattati,
                            id_asta, fo.freq_binaria, 
                            ripasso, lung_trattamento, 
                            vt.cod_percorso, vt.id_via, vt.num_seq
                            order by num_seq '''
                    
                    
                    
                    try:
                        curr1.execute(query_sit1, (max_id_macro_tappa, vv[4]))
                        logger.debug(query_sit1, max_id_macro_tappa, vv[4] )
                        sit1=curr1.fetchall()
                    except Exception as e:
                        logger.error(query_sit1, max_id_macro_tappa, vv[4] )
                        logger.error(e)
                        check_error=1
                    
                    
                        
                    macro_tappe=[]    
                    cur = con.cursor()
                    for tappa in sit1:
                        
                        
                        query_insert0='''INSERT INTO UNIOPE.CONS_MACRO_TAPPA
        (ID_MACRO_TAPPA, NOTA_VIA, QTA_TOT_SPAZZAMENTO, FREQUENZA, RIPASSO, ID_ASTA, LUNG_TRATTAMENTO)
        VALUES(:t1, :t2, :t3, :t4, :t5, :t6, :t7)'''
        
        
                        try:
                            cur.execute(query_insert0, (tappa[0], tappa[1], tappa[2], tappa[3], tappa[4], tappa[5], tappa[6]))
                            #macro_tappe.append(tappa[0])
                        except Exception as e:
                            logger.error(tappa)
                            logger.error(query_insert0)
                            logger.error(e)
                            check_error=1    
        
        
                        # dopo aver inserito le macro tappe ora mi concentro sulle micro
                        curr1 = conn.cursor()
                    
                        query_sit1=''''''
                        
                        
                        
                        
                        query_insert1='''INSERT INTO UNIOPE.CONS_PERCORSI_VIE_TAPPE
                    (ID_PERCORSO, ID_VIA, ID_TAPPA, CRONOLOGIA, DTA_IMPORT, DATA_PREVISTA)
                    VALUES(:t1, :t2, :t3, :t4, :t5, :t6)
                    '''
                    
                    
                        try:
                            cur.execute(query_insert1, (tappa[7], tappa[8], tappa[0], tappa[9], tappa[10], tappa[11]))
                            #macro_tappe.append(tappa[2])
                        except Exception as e:
                            logger.error('''{}, {},{}, {}, {}, {} '''.format(tappa[7], tappa[8], tappa[0], tappa[9], tappa[10], tappa[11]))
                            logger.error(query_insert1)
                            logger.error(e)
                            check_error=1
                    
                    # chiudo cursore oracle
                    cur.close()
                    con.commit()
                    # chiudo cursore PostgreSQL
                    curr1.close()
                   
            elif tipo_percorso == 'A':
                logger.debug('ALTRO TO DO')
                check_error=2
                stato_importazione.append('ERRORE: Percorso di tipo ALTRO non gestito dalle importazioni')
                error_mail='''Problema di importazione percorso {0}. <br> 
                Il servizio su UO è categorizzato come ALTRO
                '''    
            else:
                check_error=1
                error_mail = '''Problema di importazione percorso {0}. <br>
                Verificare funzione GETTIPOPERCORSO. <br>
                SELECT GETTIPOPERCORSO({0}, TO_DATE ({1}, 'DD/MM/YYYY')) FROM DUAL;
                '''.format(vv[0], oggi1)
                stato_importazione.append('ERRORE: Percorso di tipo sconosciuto non gestito dalle importazioni')
            
            if check_error==1 and to_import==1:
                stato_importazione.append('ERRORE: Percorso non importato correttamente')
            elif check_error==0 and to_import==1:
                # Il percorso è stato imporato ma devo replicare la consuntivazione 
                if (controllo_percorso==-1):
                    # devo lanciare una seconda procedura
                    cur = con.cursor()
                    risp='?'
                    try:
                        ret=cur.callproc('UNIOPE.REPLICACONSUNTIVAZIONE',
                                [vv[0],oggi1,risp])
                    except Exception as e:
                        #logger.error(query_o3)
                        logger.error(e)
                        
                    
                    logger.info('UNIOPE.REPLICACONSUNTIVAZIONE - ret={}'.format(ret))
                    controllo_replica=int(ret[2])
                    if controllo_replica < 0:
                        # c'è stato un errore nella funzione per replicare la consuntivazione
                        stato_importazione.append('ERRORE - La consuntivazione non è stata replicata correttamenete')
                    else:
                        stato_importazione.append('OK Percorso di tipo {} importato correttamente'.format(tipo_percorso))
                    cur.close()
                else: 
                    stato_importazione.append('OK Percorso di tipo {} importato correttamente'.format(tipo_percorso))
                    
        #exit()
        con.commit()
    

    if len(cod_percorso)!=len(stato_importazione):
        logger.error('''La lunghezza dell'array stato_importazione non è correttta. VERIFICARE ''')
    
    
    
    # chiamo la funzione per aggiornare la tabella PR_VALIDITA_PERCORSI
    cur0 = con.cursor()
    try:
        ret_func=cur0.callfunc('REP_CREADATEPERCORSI', int, [None])
        #cur1.rowfactory = makeDictFactory(cur1)
    except Exception as e:
        logger.error(sel_uo)
        logger.error(e)
    logger.info('Risposta REP_CREADATEPERCORSI={}'.format(ret_func))
    if ret_func!=0:
        logger.error('La funzione REP_CREADATEPERCORSI non ha girato correttamente')
        nota_f_mail='''<font color="red"><br><br>
        <b>ATTENZIONE</b> Ci sono stati problemi con la funzione REP_CREADATEPERCORSI di aggiornamento della tabella PR_VALIDITA_PERCORSI
        </font>'''
    elif ret_func==0: 
        nota_f_mail='<br><br>Al termine delle importazioni ha anche girato correttamente la funzione REP_CREADATEPERCORSI di aggiornamento della tabella PR_VALIDITA_PERCORSI'
    cur0.close()
    
    
    
    '''INVIO FILE VARIAZIONI PER EKOVISION'''
    curr.close()
    logger.info('Ora invio le variazioni ad EKOVISION')
    check_ekovision=0
    logger.debug(cod_percorso)
    cod_percorso_ok=tuple(cod_percorso)
    logger.debug(cod_percorso_ok)
    curr = conn.cursor()  
    
    """ SENZA TENERE CONTO  DELLE VECCHIE VERSIONI SIT
    query_variazioni_ekovision='''SELECT codice_modello_servizio, 
        case 
        when data_fine is null then ordine 
        else 1
        end
        ordine, objecy_type, 
        codice, quantita, lato_servizio, percent_trattamento,
        frequenza, numero_passaggi, nota, codice_qualita, codice_tipo_servizio,
        data_inizio, data_fine, ripasso
        FROM anagrafe_percorsi.v_percorsi_elementi_tratti
        where codice_modello_servizio = ANY (%s) and (data_inizio!=data_fine  or data_fine is null)
        order by codice_modello_servizio, data_fine, ordine,  ripasso
        '''
    """
    
    query_variazioni_ekovision='''select 
codice_modello_servizio,
coalesce((select distinct ordine from anagrafe_percorsi.v_percorsi_elementi_tratti 
where codice_modello_servizio = tab.codice_modello_servizio 
and codice = tab.codice
and ripasso = tab.ripasso and data_fine is null ),1)
as ordine,
objecy_type, 
  codice, quantita, lato_servizio, percent_trattamento,
coalesce((select distinct frequenza from anagrafe_percorsi.v_percorsi_elementi_tratti 
where codice_modello_servizio = tab.codice_modello_servizio 
and codice = tab.codice
and ripasso = tab.ripasso and data_fine is null),0)
as 
  frequenza, 
  numero_passaggi, nota,
  codice_qualita, codice_tipo_servizio,
min(data_inizio) as data_inizio, 
case 
	when max(data_fine) = '20991231' then null 
	else max(data_fine)
end data_fine, 
/*ripasso*/
case 
	when max(data_fine) = '20991231' then ripasso 
	else 0
end ripasso
from (
	  SELECT codice_modello_servizio, ordine, objecy_type, 
  codice, quantita, lato_servizio, percent_trattamento,frequenza,
  ripasso, numero_passaggi, replace(replace(coalesce(nota,''),'DA PIAZZOLA',''),';', ' - ') as nota,
  codice_qualita, codice_tipo_servizio, data_inizio, coalesce(data_fine, '20991231') as data_fine
	 FROM anagrafe_percorsi.v_percorsi_elementi_tratti where data_inizio < coalesce(data_fine, '20991231')
	 union 
	   SELECT codice_modello_servizio, ordine, objecy_type, 
  codice, quantita, lato_servizio, percent_trattamento,frequenza,
  ripasso, numero_passaggi, replace(replace(coalesce(nota,''),'DA PIAZZOLA',''),';', ' - ') as nota,
  codice_qualita, codice_tipo_servizio, data_inizio, coalesce(data_fine, '20991231') as data_fine
	 FROM anagrafe_percorsi.v_percorsi_elementi_tratti_ovs where data_inizio < coalesce(data_fine, '20991231')
 ) tab 
 where codice_modello_servizio = ANY (%s) 
 group by codice_modello_servizio,  objecy_type, 
  codice, quantita, lato_servizio, percent_trattamento,
  ripasso, numero_passaggi, nota,
  codice_qualita, codice_tipo_servizio
  order by codice_modello_servizio, data_fine asc, ordine,  ripasso'''
    
    
    #test=curr.mogrify(query_variazioni_ekovision,(cod_percorso_ok,))
    #print(test)
    #exit()
    try:
        curr.execute(query_variazioni_ekovision,(cod_percorso,))
        dettaglio_percorsi_ekovision=curr.fetchall()
    except Exception as e:
        logger.error(e)
        check_ekovision=101 # problema query
    
    try:    
        nome_csv_ekovision="variazioni_itinerari_{0}.csv".format(giorno_file)
        file_variazioni_ekovision="{0}/variazioni/{1}".format(path,nome_csv_ekovision)
        fp = open(file_variazioni_ekovision, 'w', encoding='utf-8')
        #myFile = csv.writer(fp, delimiter=';', quotechar='"', quoting=csv.QUOTE_NONNUMERIC)
        myFile = csv.writer(fp, delimiter=';')
        fieldnames = ['codice_modello_servizio', 'ordine', 'objecy_type', 
                      'codice','quantita', 'lato_servizio', 'percent_trattamento',
                      'frequenza', 'numero_passaggi', 'nota', 'codice_qualita', 'codice_tipo_servizio',
                      'data_inizio', 'data_fine', 'ripasso']
        myFile.writerow(fieldnames)
        myFile.writerows(dettaglio_percorsi_ekovision)
        fp.close()
    except Exception as e:
        logger.error(e)
        check_ekovision=102 # problema file variazioni
        
        
        
    logger.info('Invio file con le variazioni via SFTP')
    try: 
        cnopts = pysftp.CnOpts()
        cnopts.hostkeys = None
        srv = pysftp.Connection(host=url_ev_sftp, username=user_ev_sftp,
    password=pwd_ev_sftp, port= port_ev_sftp,  cnopts=cnopts,
    log="/tmp/pysftp.log")

        with srv.cd('percorsi/in/'): #chdir to public
            srv.put(file_variazioni_ekovision) #upload file to nodejs/

        # Closes the connection
        srv.close()
    except Exception as e:
        logger.error(e)
        check_ekovision=103 # problema invio SFTP
    
    
    if check_ekovision!=0:
        nota_e_mail='''<font color="red">
        <br><br><b>ATTENZIONE</b> Problema invio file delle variazioni ad EKOVISION.
        <br>Codice errore {0}
        </font>'''.format(check_ekovision)
    elif check_ekovision==0: 
        nota_e_mail='''<font color="green">
        <br><br>Il file delle variazioni è stato inviato correttamente ad EKOVISION</font>'''
    curr.close()
    
    
       
    if len(cod_percorso)>0:
        logger.info('Oggi ci sono {} variazioni. Creo nuovo file'.format(len(cod_percorso)))
        nome_file="{0}_variazioni.xlsx".format(giorno_file)
        file_variazioni="{0}/variazioni/{1}".format(path,nome_file)
        
        
        workbook = xlsxwriter.Workbook(file_variazioni)
        w = workbook.add_worksheet()

        w.write(0, 0, 'cod_percorso') 
        w.write(0, 1, 'descrizione') 
        w.write(0, 2, 'servizio') 
        w.write(0, 3, 'ut') 
        w.write(0, 4, 'ESITO IMPORTAZIONE') 
        w.write(0, 5, 'MAIL') 
        
        '''
        w.write(1, 0, 1234.56)  # Writes a float
        w.write(2, 0, 'Hello')  # Writes a string
        w.write(3, 0, None)     # Writes None
        w.write(4, 0, True)     # Writes a bool
        '''
        
        #f = open(file_variazioni, "w")
        #f.write('cod_percorso;descrizione;servizio;ut_resp\n')
    


    i=0
    while i<len(cod_percorso):
        #f.write('"{}";"{}";"{}";"{}"\n'.format(cod_percorso[i],descrizione[i],servizio[i],ut[i]))
        w.write(i+1,0,'{}'.format(cod_percorso[i]))
        w.write(i+1,1,'{}'.format(descrizione[i]))
        w.write(i+1,2,'{}'.format(servizio[i]))
        w.write(i+1,3,'{}'.format(ut[i]))
        w.write(i+1,4,'{}'.format(stato_importazione[i]))
        w.write(i+1,5,'{}'.format(invio_mail[i]))
        # provo l'invio dei report 
        try:
            if stato_importazione[i].lower().startswith("ok"):              
                report_settimanali_percorsi_ok.main(cod_percorso[i], 'sempl', invio_mail[i], num)
        except Exception as e:
            logger.error(e)
            
        i+=1

    if len(cod_percorso)>0:
        #f.close()
        workbook.close()

    #exit() # per ora esco qua e non vado oltre

    
    # Create a secure SSL context
    context = ssl.create_default_context()



   # messaggio='Test invio messaggio'


    subject = "Variazioni odierne - File automatico"
    if num==1:
        gg_text='''dell'ultimo giorno (ieri)'''
    else:
        gg_text='''degli ultimi {} giorni'''.format(num)
        
        
    riepilogo_controlli='''
    <br><br><b>Riepilogo check</b>:
    <br> check_error= {0}
    <br> check_ekovision= {1}
    '''.format(check_error, check_ekovision)
       
    body = """Report giornaliero delle variazioni {0}.<br><br>
    
    I nuovi percorsi sono già stati importati. Non dovrebbe servire nessuna verifica.
    {3}
    {4}
    <br>
    
    <b>Riepilogo controlli  - TEST </b>
    {5}
    <br><br><br>
    L'applicativo che gestisce le importazioni su UO in maniera automatica è stato realizzato dal gruppo Gestione Applicativi del SIGT.<br> 
    Segnalare tempestivamente eventuali malfunzionamenti inoltrando la presente mail a {1}<br><br>
    Giorno {2}<br><br>
    AMIU Assistenza Territorio<br>
     <img src="cid:image1" alt="Logo" width=197>
    <br>
    """.format(gg_text, user_mail, oggi1, nota_f_mail, nota_e_mail, riepilogo_controlli)
    ##sender_email = user_mail
    receiver_email='assterritorio@amiu.genova.it'
    debug_email='roberto.marzocchi@amiu.genova.it'

    # Create a multipart message and set headers
    message = MIMEMultipart()
    message["From"] = sender_email
    message["To"] = receiver_email
    message["Subject"] = subject
    #message["Bcc"] = debug_email  # Recommended for mass emails
    message.preamble = "File giornaliero con le variazioni"


        
                        
    # Add body to email
    message.attach(MIMEText(body, "html"))


    #aggiungo logo 
    logoname='{}/img/logo_amiu.jpg'.format(path)
    immagine(message,logoname)
    
    
    # aggiunto allegato (usando la funzione importata)
    allegato(message, file_variazioni, nome_file)
    

    
    
    text = message.as_string()

    logger.info("Richiamo la funzione per inviare mail")
    invio=invio_messaggio(message)
    logger.info(invio)
    
    
    
     
        
    ##################################################################################################
    ####                                SISTEMO LE SEQUENZE 
    ##################################################################################################
    
    cur0 = con.cursor()
    cur1 = con.cursor()
    cur2 = con.cursor()
    
    
    sel_uo0='''SELECT last_number
    FROM user_sequences
    WHERE sequence_name ='SEQ_ID_MACRO_TAPPA' ''' 
    try:
        cur0.execute(sel_uo0)
        #cur1.rowfactory = makeDictFactory(cur1)
        current_seq=cur0.fetchall()
    except Exception as e:
        logger.error(sel_uo)
        logger.error(e)
        
    cur0.close() 
    
    
    sel_uo='''SELECT max(ID_MACRO_TAPPA) FROM CONS_MACRO_TAPPA cmt ''' 
    try:
        cur1.execute(sel_uo)
        #cur1.rowfactory = makeDictFactory(cur1)
        max_macro=cur1.fetchall()
    except Exception as e:
        logger.error(sel_uo)
        logger.error(e)
    
    max=max_macro[0][0]
    
    cur1.close()      

    if (current_seq[0][0]<max):
        check=0
        logger.debug('La sequenza è da correggere')
    else:
        logger.debug('La sequenza è OK, non devo fare nulla')
        check=2
    
    if check ==0:
        logger.info(' Faccio un ciclo per portare la sequenza fino al valore massimo di {}'.format(max))
    
    
    while check==0:
        sel_uo2='''select SEQ_ID_MACRO_TAPPA.NEXTVAL from dual'''
        try:
            cur2.execute(sel_uo2)
            #cur1.rowfactory = makeDictFactory(cur1)
            seq_macro=cur2.fetchall()
        except Exception as e:
            logger.error(sel_uo2)
            logger.error(e)
        logger.debug('max={} macro={}'.format(max,seq_macro[0][0]))
        if seq_macro[0][0]== max:
            check=1
        #exit()
    

    cur2.close()      
    
    
    
    
    ## MICRO TAPPE
    
    cur0 = con.cursor()
    cur1 = con.cursor()
    cur2 = con.cursor()
    
    
    sel_uo0='''SELECT last_number
    FROM user_sequences
    WHERE sequence_name ='SEQ_ID_MICRO_TAPPA' ''' 
    try:
        cur0.execute(sel_uo0)
        #cur1.rowfactory = makeDictFactory(cur1)
        current_seq=cur0.fetchall()
    except Exception as e:
        logger.error(sel_uo)
        logger.error(e)
        
    cur0.close() 
    
    
    sel_uo='''SELECT max(ID_MICRO_TAPPA) FROM CONS_MICRO_TAPPA cmt ''' 
    try:
        cur1.execute(sel_uo)
        #cur1.rowfactory = makeDictFactory(cur1)
        max_micro=cur1.fetchall()
    except Exception as e:
        logger.error(sel_uo)
        logger.error(e)
    
    max=max_micro[0][0]
    
    cur1.close()      

    if (current_seq[0][0]<max):
        check=0
        logger.debug('La sequenza è da correggere')
    else:
        logger.debug('La sequenza è OK, non devo fare nulla')
        check=2
    
    if check ==0:
        logger.info(' Faccio un ciclo per portare la sequenza fino al valore massimo di {}'.format(max))
    
    
    while check==0:
        sel_uo2='''select SEQ_ID_MICRO_TAPPA.NEXTVAL from dual'''
        try:
            cur2.execute(sel_uo2)
            #cur1.rowfactory = makeDictFactory(cur1)
            seq_micro=cur2.fetchall()
        except Exception as e:
            logger.error(sel_uo2)
            logger.error(e)
        logger.debug('max={} micro={}'.format(max,seq_micro[0][0]))
        if seq_micro[0][0]== max:
            check=1
        #exit()
    

    cur2.close()
    
    
    ##################################################################################################
    ####                                UPDATE mv_percorsi_elementi_tratti_dismessi
    ##################################################################################################

    curr.close()
    logger.info('Ora FACCIO REFRESH MATERIALIZED VIEW anagrafe_percorsi.mv_percorsi_elementi_tratti_dismessi')
    curr = conn.cursor()

    r_sql= '''REFRESH MATERIALIZED VIEW anagrafe_percorsi.mv_percorsi_elementi_tratti_dismessi'''
    try:
        curr.execute(r_sql)
    except Exception as e:
        logger.error(e)
        check_ekovision=101 # problema query
    
    conn.commit()   
    
    ##################################################################################################
    #                               CHIUDO LE CONNESSIONI
    ################################################################################################## 
    logger.info("Chiudo definitivamente le connesioni al DB")
    con.close()
    conn.close()

    # check se c_handller contiene almeno una riga 
    error_log_mail(errorfile, 'roberto.marzocchi@amiu.genova.it', os.path.basename(__file__), logger)

if __name__ == "__main__":
    main()