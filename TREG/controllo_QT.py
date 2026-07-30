#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2026
# Roberta Fagandini, Roberto Marzocchi

'''
Scopo dello script è 

1) Porta i dati di TREG su SIT
2) Controlla discrepanze tra TREG e SIT
3) Manda mail di warning se ci sono discrepanze 


Per connettermi a server TREG servono driver Microsoft ODBC, che su Linux si installano così:

curl -sSL -O https://packages.microsoft.com/config/ubuntu/22.04/packages-microsoft-prod.deb
sudo dpkg -i packages-microsoft-prod.deb
rm packages-microsoft-prod.deb
sudo apt update
sudo ACCEPT_EULA=Y apt install msodbcsql18

'''

#from msilib import type_short
import os, sys, re  # ,shutil,glob
import inspect
from pathlib import Path

import requests
from requests.exceptions import HTTPError

import json


#import getopt  # per gestire gli input

#import pymssql



import locale

import xlsxwriter

# per leggere file excel
#from python_calamine import CalamineWorkbook
import openpyxl


import psycopg2

import pyodbc

#import cx_Oracle

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

import uuid


#import datetime 
from datetime import date, datetime, timedelta

import holidays



filename = inspect.getframeinfo(inspect.currentframe()).filename
path=os.path.dirname(sys.argv[0]) 
path1 = os.path.dirname(os.path.dirname(os.path.abspath(filename)))
nome=os.path.basename(__file__).replace('.py','')
#tmpfolder=tempfile.gettempdir() # get the current temporary directory
logfile='{0}/log/{1}.log'.format(path,nome)
errorfile='{0}/log/error_{1}.log'.format(path,nome)
#if os.path.exists(logfile):
#    os.remove(logfile)








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

import time

#variabile che specifica se devo fare test ekovision oppure no
test_ekovision=0
    

# da capire come gestisce datetime e date
    
def normalize(v):
    if v is None:
        return None

    # date / datetime -> lascia così (psycopg2 li gestisce)
    if isinstance(v, (datetime, date)):
        return v
    
    # stringhe
    if isinstance(v, str):
        v = v.strip()

        if v == "":
            return None

        if v.lower() in ("sì", "si"):
            return True

        if v.lower() == "no":
            return False

        # prova a convertire una data/ora
        try:
            dt = datetime.strptime(v, "%d/%m/%Y %H:%M")
            if dt.hour == 0 and dt.minute == 0:
                return dt.date()
            else:
                return dt
        except ValueError:
            pass
        
        return v

    # float tipo 10.0 -> 10
    if isinstance(v, float):
        if v.is_integer():
            return int(v)
        return False
        
    

    
    return v



def main():


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
    
    
    
    logger.info('Il PID corrente è {0}'.format(os.getpid()))

    

    # Get today's date
    #presentday = datetime.now() # or presentday = datetime.today()
    oggi=datetime.today()
    #logging.debug('Oggi {}'.format(oggi))
    
    oggi_char=oggi.strftime('%Y%m%d%H%M%S')
    
    
    
    
    ###################################################################
    # variabili per controllo
    anno_controllo=2026
    mesi_controllo = [1,2,3,4,5,6]
    ###################################################################
    
    
    
    # TO DO 
    # aggiungere ciclo sui comuni
    
    ################################################################
    # QUERY
    ################################################################
    raccolta_TREG = '''SELECT code_rint, 
code_area, code_street, dt_prg_ini, dt_prg_fin,
dt_exe_ini, dt_exe_fin,
cause_int
FROM twsTRG.ATRIF.anwstcol
WHERE istat = ?
and dt_prg_ini between 
convert(DATETIME,
	?, 
	103) 
	and convert(DATETIME,
	?, 
	103) 
order by dt_prg_ini'''
    
    
    spazzamento_TREG='''SELECT code_rint, 
code_area, code_street, dt_prg_ini, dt_prg_fin,
dt_exe_ini, dt_exe_fin,
cause_int
FROM twsTRG.ATRIF.answeep
WHERE istat = ?
and dt_prg_ini between 
convert(DATETIME,
	?, 
	103) 
	and convert(DATETIME,
	?, 
	103) 
order by dt_prg_ini '''
    
    
    
   
    # per passare la tupla devo usare execute_values, altrimenti mi da errore di sintassi
    from psycopg2.extras import execute_values
    insert_ev_racc='''INSERT INTO treg_eko.anwstcol (
        code_rint, code_area, 
        code_street, dt_prg_ini, 
        dt_prg_fin, dt_exe_ini, 
        dt_exe_fin, cause_int) 
        VALUES %s;'''
    
    
    insert_ev_spazz='''INSERT INTO treg_eko.answeep (
            code_rint, code_area, 
            code_street, dt_prg_ini, 
            dt_prg_fin, dt_exe_ini, 
            dt_exe_fin, cause_int) 
            VALUES %s;'''
    
    # tabella temporanea per confronti 
    drop_tmp_table_racc= ''' drop table treg_eko.tmp_raccolta'''
    # tabella temporanea per confronti 
    drop_tmp_table_spazz= ''' drop table treg_eko.tmp_spazzamento'''
    
    create_tmp_table_racc='''create table treg_eko.tmp_raccolta as 
select 
	    distinct
	    c.descr_comune as comune,
	    case 
		    when ep.giorno_competenza = 0 then extract(year from data_programmata)
		    when ep.giorno_competenza = -1 then extract(year from data_programmata-1)
	    end anno,
	    case 
	        when ep.giorno_competenza = 0 then extract(month from data_programmata)
	        when ep.giorno_competenza = -1 then extract(month from data_programmata-1)
	    end mese, 
	    case 
	        when (tipo_raccolta = 'OTH' and tempo_recupero > 72)
	        or 
	        (tipo_raccolta in ('DOM', 'PRG') and tempo_recupero > 24)
	        then 1
	        else 0
	    end interruzione,
	    case 
	        when tempo_ripresa >= 24
	        then 1
	        else 0
	    end disservizio,
	    trac_code, 
	    cd.codice,
	    cd.descrizione as causale,
	    ca.id as id_causale_arera,
	    ca.descrizione as causale_arera
	    from consunt.report_raccolta rr
	    join topo.vie v on v.id_via = rr.id_via
	    join topo.comuni c on c.id_comune= v.id_comune
	    join anagrafe_percorsi.elenco_percorsi ep on ep.cod_percorso = rr.cod_percorso
	        and data_programmata between ep.data_inizio_validita and ep.data_fine_validita - 1
	    join etl.cause_disserv cd on cd.codice = rr.id_causale
	    left join etl.causali_arera ca on cd.id_causale_arera = ca.id
	    join anagrafe_percorsi.anagrafe_tipo at2 on at2.id = ep.id_tipo
	    where rr.non_previsto is null
	    and at2.gestione_arera = true 
	    and rr.id_causale not in (101,102, 999)
	    and data_programmata between to_date(%s, 'DD/MM/YYYY' )
	    	and to_date(%s, 'DD/MM/YYYY' )
	    and c.id_comune = %s'''
    
    
    
    create_tmp_table_spazz='''create table treg_eko.tmp_spazzamento as 
    select distinct 
c.descr_comune as comune,
case 
	    when ep.giorno_competenza = 0 then extract(year from data_programmata)
	    when ep.giorno_competenza = -1 then extract(year from data_programmata-1)
    end anno,
    case 
        when ep.giorno_competenza = 0 then extract(month from data_programmata)
        when ep.giorno_competenza = -1 then extract(month from data_programmata-1)
    end mese, 
case 
	when coalesce(rr.tempo_recupero, 0) > 24
	then 1
	else 0
end interruzione,
case 
	when coalesce(tempo_ripresa,0) >= 24
	then 1
	else 0
end disservizio,
trac_code, 
rr.lung_km as lung_km, 
cd.codice, 
cd.descrizione as causale,
cd.id_causale_arera
--min(cd.codice) as codice,
--min(cd.descrizione) as causale,
--max(cd.id_causale_arera) as id_causale_arera
from consunt.report_spazz rr
join topo.vie v on v.id_via = rr.id_via
join topo.comuni c on c.id_comune= v.id_comune
join anagrafe_percorsi.elenco_percorsi ep on ep.cod_percorso = rr.cod_percorso
	and data_programmata between ep.data_inizio_validita and ep.data_fine_validita - 1
join etl.cause_disserv cd on cd.codice = rr.id_causale
--left join etl.causali_arera ca on cd.id_causale_arera = ca.id
join anagrafe_percorsi.anagrafe_tipo at2 on at2.id = ep.id_tipo
where rr.non_previsto is null
and at2.gestione_arera = true 
and rr.id_causale not in (101,102,999)
    and data_programmata between to_date(%s, 'DD/MM/YYYY' )
                and to_date(%s, 'DD/MM/YYYY' )
            and c.id_comune = %s'''
    
    
    # differenze pianificati
    #sit non treg
    pian_r_sit_no_treg = '''select distinct(trac_code)
    from treg_eko.tmp_raccolta
    where anno = %s and mese = %s 
    and trac_code not in (select code_rint FROM treg_eko.anwstcol)
    '''

    pian_r_treg_no_sit = ''' 
    SELECT distinct code_rint FROM treg_eko.anwstcol
    where code_rint not in (select distinct(trac_code)
    from treg_eko.tmp_raccolta
    where anno = %s and mese = %s )
    '''
    
    
    
    pian_s_sit_no_treg = '''select distinct(trac_code)
        from treg_eko.tmp_spazzamento
        where anno = %s and mese = %s 
        and trac_code not in (select code_rint FROM treg_eko.answeep)
        '''
    
    pian_s_treg_no_sit = ''' 
        SELECT distinct code_rint FROM treg_eko.answeep
        where code_rint not in (select distinct(trac_code)
        from treg_eko.tmp_spazzamento
        where anno = %s and mese = %s )'''
    
    
    
    
    
    
    
    select_tipi_causale='''select id, id_treg, descrizione from etl.causali_arera ca where id > 0'''
    
    select_comune='''select id_comune, c.cod_istat, c.descr_comune 
from topo.comuni c  where c.gestito_sit = 'S' '''
    
    # serrve per provare a riprocessare in automatico i trac_code presenti da una parte e non dall'altra
    update_lastupdate_trac_code='''update treg_eko.consunt_ekovision ce 
    set data_last_update = now()
    where codice = %s and data_pianif_iniziale= %s'''
    
    # connessione a SIT
    nome_db=db
    logger.info('Connessione al db {}'.format(nome_db))
    conn = psycopg2.connect(dbname=nome_db,
                        port=port,
                        user=user,
                        password=pwd,
                        host=host)

    curr = conn.cursor()
      
      
    # connessione a TREG 
    logger.info('Connessione al db {}'.format(db_treg))
    connt = pyodbc.connect(
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={host_db_treg},{port_db_treg};"
        f"DATABASE={db_treg};"
        f"UID={user_db_treg};"
        f"PWD={pwd_db_treg};"
        "TrustServerCertificate=yes;"
    )
    
    # da qualche parte salveremo i file excel processati in modo da non processarli più di una volta
    currt = connt.cursor()
    
    
    
    # faccio un ciclo sui mesi
    #excel_files_array =[]
    excel_names_array =[]
    for mm in mesi_controllo:
        logger.info(F'Sto facendo il controllo per il mese {mm} del {anno_controllo}')
        messaggio=''
        data_inizio=datetime(anno_controllo, mm, 1).strftime("%d/%m/%Y")
        data_fine=datetime(anno_controllo, mm+1, 1).strftime("%d/%m/%Y")

    
        # PREDISPONGO IL FILE EXCEL 
        mese_file=str(mm).rjust(2,'0')
        nome_excel=f'anomalie_{anno_controllo}{mese_file}.xlsx' 
        excel_names_array.append(nome_excel)
        nome_file_excel=f'{path}/{nome_excel}' 
        workbook = xlsxwriter.Workbook(nome_file_excel)
        
        
        worksheet = workbook.add_worksheet('Anomalie RACC')
        worksheet2 = workbook.add_worksheet('Anomalie SPAZZ')
        
        
        header_format = workbook.add_format({
            'bold': True,
            'bg_color': '#D9EAD3'
        })

        # fuori dal ciclo, una volta sola
        date_format = workbook.add_format({'num_format': 'dd/mm/yyyy hh:mm:ss'})
        
        # per raccolta
        row_excel = 0
        s=0
    
        # per spazzamento
        row_excel2 = 0
        s2=0

        try:
            curr.execute(select_comune)
            comuni = curr.fetchall()
        except Exception as e:
            check_error=1
            logger.error(select_comune)
            logger.error(e)
            # se va in errore esco
            error_log_mail(errorfile, 'assterritorio@amiu.genova.it', os.path.basename(__file__), logger)
            exit()

        for com in comuni:
            logger.info(f'Controllo comune di {com[2]}')
            
            logger.info('Truncate copie TREG su SIT')            
            # racc
            curr.execute("TRUNCATE TABLE treg_eko.anwstcol")
            #spazz
            curr.execute("TRUNCATE TABLE treg_eko.answeep")


            logger.info('Copio nuovamente i dati TREG su SIT')

            ####################################################################
            # RACC
            try: 
                currt.execute(raccolta_TREG, (com[1], data_inizio, data_fine))
            except Exception as e:
                check_error=1
                logger.error(raccolta_TREG)
                logger.error(e)
                # se va in errore esco
                error_log_mail(errorfile, 'assterritorio@amiu.genova.it', os.path.basename(__file__), logger)
                exit()
            
            
            
            # per evitare di intasare la RAM  faccio commit di 10'000 righe alla volta
            batch_size =10000
            contatore = 0
            while True:
                contatore += batch_size
                #logger.info(f'Copiate {contatore} righe nella tabella treg_eko.anwstcol') 
                rows = currt.fetchmany(batch_size)

                if not rows:
                    break

                execute_values(
                    curr,insert_ev_racc,
                    rows,
                    page_size=batch_size
                )



            ####################################################################
            # SPAZZ

            try: 
                currt.execute(spazzamento_TREG, (com[1], data_inizio, data_fine))
            except Exception as e:
                check_error=1
                logger.error(spazzamento_TREG)
                logger.error(e)
                # se va in errore esco
                error_log_mail(errorfile, 'assterritorio@amiu.genova.it', os.path.basename(__file__), logger)
                exit()
            
            
            
            # per evitare di intasare la RAM  faccio commit di 10'000 righe alla volta
            batch_size =10000
            contatore = 0
            while True:
                contatore += batch_size
                #logger.info(f'Copiate {contatore} righe nella tabella treg_eko.anwstcol') 
                rows = currt.fetchmany(batch_size)

                if not rows:
                    break

                execute_values(
                    curr,insert_ev_spazz,
                    rows,
                    page_size=batch_size
                )




            conn.commit()




            logger.info('cancello e ricreo la tabella su cui fare i confronti')
            # creo la tabella temporanea sul SIT su cui fare i confronti
            try: 
                curr.execute(drop_tmp_table_racc)
            except Exception as e:
                check_error=1
                logger.error(drop_tmp_table_racc)
                logger.error(e)
                # se va in errore esco
                error_log_mail(errorfile, 'assterritorio@amiu.genova.it', os.path.basename(__file__), logger)
                exit()
            
            
            try: 
                curr.execute(drop_tmp_table_spazz)
            except Exception as e:
                check_error=1
                logger.error(drop_tmp_table_spazz)
                logger.error(e)
                # se va in errore esco
                error_log_mail(errorfile, 'assterritorio@amiu.genova.it', os.path.basename(__file__), logger)
                exit()
            
            
            try: 
                curr.execute(create_tmp_table_racc, (data_inizio, data_fine, com[0])) 
            except Exception as e:
                check_error=1
                logger.error(create_tmp_table_racc)
                logger.error(e)
                # se va in errore esco
                error_log_mail(errorfile, 'assterritorio@amiu.genova.it', os.path.basename(__file__), logger)
                exit()
            
            
            try: 
                curr.execute(create_tmp_table_spazz, (data_inizio, data_fine, com[0])) 
            except Exception as e:
                check_error=1
                logger.error(create_tmp_table_spazz)
                logger.error(e)
                # se va in errore esco
                error_log_mail(errorfile, 'assterritorio@amiu.genova.it', os.path.basename(__file__), logger)
                exit()
            
            conn.commit()
            
            logger.info('Controllo pianificati raccolta')
             
            # controllo pianificati 
            try:
                curr.execute(pian_r_sit_no_treg, (anno_controllo, mm))
                risultato=curr.fetchall()
            except Exception as e:
                check_error=1
                logger.error(pian_r_sit_no_treg)
                logger.error(e)
                # se va in errore esco
                error_log_mail(errorfile, 'assterritorio@amiu.genova.it', os.path.basename(__file__), logger)
                exit()

            
            
            if len(risultato) > 0 :
                messaggio = f'{messaggio}\n ❌ per il comune {com[2]} ci sono dei servizi pianificati presenti su SIT e non su TREG'
                
                messaggio= f'{messaggio}\nTrac codes: '
                for rr in risultato: 
                    curr.execute(update_lastupdate_trac_code, (rr[0].split('_')[0],rr[0].split('_')[1],))
                    messaggio=f'{messaggio} {rr[0]}'
                conn.commit()
                # bisogna fare elenco
            else:
                messaggio = f'{messaggio}\n ✔ Tutti i pianificati presenti su SIT sono anche su TREG (comune {com[2]})'
            
            
            
            
            
            
            try:
                curr.execute(pian_r_treg_no_sit, (anno_controllo, mm))
                risultato=curr.fetchall()
            except Exception as e:
                check_error=1
                logger.error(pian_r_treg_no_sit)
                logger.error(e)
                # se va in errore esco
                error_log_mail(errorfile, 'assterritorio@amiu.genova.it', os.path.basename(__file__), logger)
                exit()

            
            
            if len(risultato) > 0 :
                messaggio = f'{messaggio}\n ❌ per il comune {com[2]} ci sono dei servizi pianificati presenti su TREG e non su SIT'
                
                messaggio= f'{messaggio}\nTrac codes: '
                for rr in risultato: 
                    curr.execute(update_lastupdate_trac_code, (rr[0].split('_')[0],rr[0].split('_')[1],))
                    messaggio=f'{messaggio} {rr[0]}'
                conn.commit()
                # bisogna fare elenco
            else:
                messaggio = f'{messaggio}\n ✔ Tutti i pianificati presenti su TREG sono anche su SIT (comune {com[2]})'    
            
            
            
            
            
            
            
            logger.info('Controllo pianificati Spazzamento')          
            
            try:
                curr.execute(pian_s_sit_no_treg, (anno_controllo, mm))
                risultato=curr.fetchall()
            except Exception as e:
                check_error=1
                logger.error(pian_s_sit_no_treg)
                logger.error(e)
                # se va in errore esco
                error_log_mail(errorfile, 'assterritorio@amiu.genova.it', os.path.basename(__file__), logger)
                exit()

            
            
            if len(risultato) > 0 :
                messaggio = f'{messaggio}\n ❌ per il comune {com[2]} ci sono dei servizi pianificati presenti su SIT e non su TREG'
                
                messaggio= f'{messaggio}\nTrac codes: '
                for rr in risultato: 
                    curr.execute(update_lastupdate_trac_code, (rr[0].split('_')[0],rr[0].split('_')[1],))
                    messaggio=f'{messaggio} {rr[0]}'
                conn.commit()
                # bisogna fare elenco
            else:
                messaggio = f'{messaggio}\n ✔ Tutti i pianificati presenti su SIT sono anche su TREG (comune {com[2]})'
            
            
            
            
            
            
            try:
                curr.execute(pian_s_treg_no_sit, (anno_controllo, mm))
                risultato=curr.fetchall()
            except Exception as e:
                check_error=1
                logger.error(pian_s_treg_no_sit)
                logger.error(e)
                # se va in errore esco
                error_log_mail(errorfile, 'assterritorio@amiu.genova.it', os.path.basename(__file__), logger)
                exit()

            
            
            if len(risultato) > 0 :
                messaggio = f'{messaggio}\n ❌ per il comune {com[2]} ci sono dei servizi pianificati presenti su TREG e non su SIT'
                
                messaggio= f'{messaggio}\nTrac codes: '
                for rr in risultato: 
                    curr.execute(update_lastupdate_trac_code, (rr[0].split('_')[0],rr[0].split('_')[1],))
                    messaggio=f'{messaggio} {rr[0]}'
                conn.commit()
                # bisogna fare elenco
            else:
                messaggio = f'{messaggio}\n ✔ Tutti i pianificati presenti su TREG sono anche su SIT (comune {com[2]})'    
            

            
            
            
            
            
            # ora faccio ciclo sulle causali
            
            try:
                curr.execute(select_tipi_causale)
                tipi_causale= curr.fetchall()
            except Exception as e:
                logger.error(select_tipi_causale)
                logger.error(e)
            
            
        
        
            for cc in tipi_causale:
                
                
                
                anomalie=[]
                
                logger.info(f'Cerco anomali raccolta mese {mm} anno {anno_controllo} causale TREG {cc[1]}')
                select_anomalie='''
                    with anomalie as 
                    (
                        SELECT  '{1}' as comune,
                        'TREG NO SIT' as tipo_anomalia,
                        '{0}' as tipo_TREG,
                        ca.id_TREG as tipo_SIT,
                        rr.* 
                        FROM treg_eko.anwstcol a
                        left join consunt.report_raccolta rr on a.code_rint = rr.trac_code  
                        left join etl.cause_disserv cd on cd.codice = rr.id_causale 
                        left join etl.causali_arera ca on cd.id_causale_arera = ca.id 
                        WHERE cause_int IN (%s)	
                        and a.code_rint not in (
                        select distinct trac_code 
                        FROM treg_eko.tmp_raccolta
                        where anno = %s and mese = %s and id_causale_arera = %s and interruzione = 1)
                        union
                        SELECT  '{1}' as comune,
                        'SIT NO TREG' as tipo_anomalia,
                        a.cause_int as tipo_TREG,
                        '{0}' as tipo_SIT,
                        rr.* 
                        FROM treg_eko.tmp_raccolta tr
                        left join consunt.report_raccolta rr on tr.trac_code = rr.trac_code  
                        left join treg_eko.anwstcol a on a.code_rint = tr.trac_code  
                        where anno = %s and mese = %s and id_causale_arera = %s and interruzione = 1
                        and  tr.trac_code not in (select code_rint 
                        FROM treg_eko.anwstcol a
                        WHERE cause_int IN (%s))
                    )
                    select * from anomalie 
                    order by data_programmata, cod_percorso'''.format(cc[1],com[2])
                try:
                    curr.execute(select_anomalie, (cc[1], 
                                                    anno_controllo, 
                                                    mm, 
                                                    cc[0],
                                                    anno_controllo, 
                                                    mm, 
                                                    cc[0],
                                                    cc[1]
                                                    ))
                    anomalie=curr.fetchall()
                except Exception as e:
                    logger.error(select_anomalie)
                    logger.error(e)
                
                
                if len(anomalie) > 0 :
                    messaggio = f'{messaggio}\n ❌ ci sono anomalie con causale {cc[2]}'
                    
                    """messaggio= f'{messaggio}\nTrac codes: '
                    for rr in risultato: 
                        messaggio=f'{messaggio} {rr[0]}'
                    # bisogna fare elenco
                    """
                else:
                    messaggio = f'{messaggio}\n ✔ Con la causale {cc[1]} tutto torna!'
                # step 0 devo scrivere intestazione, dopo solo append
                # Scrittura intestazione una sola volta
                if s == 0:

                    colonne = [desc[0] for desc in curr.description]

                    for col_num, col_name in enumerate(colonne):
                        worksheet.write(row_excel, col_num, col_name, header_format)

                    row_excel += 1

                # Scrittura dati
                for record in anomalie:

                    for col_num, valore in enumerate(record):
                        #worksheet.write(row_excel, col_num, valore)
                        # prima di scrivere controllo se si tratta di una data. 
                        # A quel punto imposto il formato corretto su excel
                        if isinstance(valore, (datetime, date)):
                            worksheet.write_datetime(row_excel, col_num, valore, date_format)
                        else:
                            worksheet.write(row_excel, col_num, valore)
                        

                    row_excel += 1

                # incremento s
                s+=1

            
            
            
            
                logger.info(f'Cerco anomali Spazzamento mese {mm} anno {anno_controllo} causale TREG {cc[1]}')
                select_anomalie='''
                    with anomalie as 
                    (
                        SELECT '{1}' as comune, 
                        'TREG NO SIT' as tipo_anomalia,
                        '{0}' as tipo_TREG,
                        ca.id_TREG as tipo_SIT,
                        rr.* 
                        FROM treg_eko.answeep a
                        left join consunt.report_spazz rr on a.code_rint = rr.trac_code  
                        left join etl.cause_disserv cd on cd.codice = rr.id_causale 
                        left join etl.causali_arera ca on cd.id_causale_arera = ca.id 
                        WHERE cause_int IN (%s)	
                        and a.code_rint not in (
                        select distinct trac_code 
                        FROM treg_eko.tmp_spazzamento
                        where anno = %s and mese = %s and id_causale_arera = %s and interruzione = 1)
                        union
                        SELECT  '{1}' as comune,
                        'SIT NO TREG' as tipo_anomalia,
                        a.cause_int as tipo_TREG,
                        '{0}' as tipo_SIT,
                        rr.* 
                        FROM treg_eko.tmp_spazzamento tr
                        left join consunt.report_spazz rr on tr.trac_code = rr.trac_code  
                        left join treg_eko.answeep a on a.code_rint = tr.trac_code  
                        where anno = %s and mese = %s and id_causale_arera = %s and interruzione = 1
                        and  tr.trac_code not in (select code_rint 
                        FROM treg_eko.answeep a
                        WHERE cause_int IN (%s))
                    )
                    select * from anomalie 
                    order by data_programmata, cod_percorso'''.format(cc[1], com[2])
                try:
                    curr.execute(select_anomalie, (cc[1], 
                                                    anno_controllo, 
                                                    mm, 
                                                    cc[0],
                                                    anno_controllo, 
                                                    mm, 
                                                    cc[0],
                                                    cc[1]
                                                    ))
                    anomalie=curr.fetchall()
                except Exception as e:
                    logger.error(select_anomalie)
                    logger.error(e)
                
                
                if len(anomalie) > 0 :
                    messaggio = f'{messaggio}\n ❌ ci sono anomalie con causale {cc[2]}'
                    
                    """messaggio= f'{messaggio}\nTrac codes: '
                    for rr in risultato: 
                        messaggio=f'{messaggio} {rr[0]}'
                    # bisogna fare elenco
                    """
                else:
                    messaggio = f'{messaggio}\n ✔ Con la causale {cc[1]} tutto torna!'
                # step 0 devo scrivere intestazione, dopo solo append
                # Scrittura intestazione una sola volta
                if s2 == 0:

                    colonne2 = [desc[0] for desc in curr.description]

                    for col_num, col_name in enumerate(colonne2):
                        worksheet2.write(row_excel2, col_num, col_name, header_format)

                    row_excel2 += 1

                # Scrittura dati
                for record in anomalie:

                    for col_num, valore in enumerate(record):
                        #worksheet.write(row_excel, col_num, valore)
                        # prima di scrivere controllo se si tratta di una data. 
                        # A quel punto imposto il formato corretto su excel
                        if isinstance(valore, (datetime, date)):
                            worksheet2.write_datetime(row_excel2, col_num, valore, date_format)
                        else:
                            worksheet2.write(row_excel2, col_num, valore)
                        

                    row_excel2 += 1

                # incremento s
                s2+=1
        
        
        
        # fine ciclo sui comuni        
        # Autofilter
        if row_excel > 1:
            worksheet.autofilter(
                0,
                0,
                row_excel - 1,
                len(colonne) - 1
            )

        # Congela prima riga
        worksheet.freeze_panes(1, 0)


        # Autofit colonne
        for idx, nome_colonna in enumerate(colonne):
            max_len = len(str(nome_colonna))

            # Se vuoi un autofit "vero" bisogna memorizzare le lunghezze durante la scrittura.
            # Qui imposto una larghezza minima ragionevole.
            worksheet.set_column(idx, idx, max(max_len + 5, 15))






        if row_excel2 > 1:
            worksheet2.autofilter(
                0,
                0,
                row_excel2 - 1,
                len(colonne2) - 1
            )
                    
        # Congela prima riga
        worksheet2.freeze_panes(1, 0)

        # Autofit colonne
        for idx, nome_colonna in enumerate(colonne2):
            max_len = len(str(nome_colonna))

            # Se vuoi un autofit "vero" bisogna memorizzare le lunghezze durante la scrittura.
            # Qui imposto una larghezza minima ragionevole.
            worksheet2.set_column(idx, idx, max(max_len + 5, 15))

        
        # chiudo il file excel
        workbook.close()
        
        
        
        logger.info(messaggio)
        #exit()

    
    
    
    
    ################################
    # predisposizione mail
    ################################
    # Create a secure SSL context
    context = ssl.create_default_context()


    subject = "Controllo settimanale anomalie TREG"
    body = f'''
Visualizza in allegato il controllo effettuato sui seguenti periodi: 
<ul>
<li>anno = {anno_controllo}</li>
<li>mesi = {mesi_controllo}</li>
</ul>
<br><br>
AMIU Assistenza Territorio
'''
    

    # Create a multipart message and set headers
    message = MIMEMultipart()
    message["From"] = sender_email
    message["To"] = 'assterritorio@amiu.genova.it'
    #message["Cc"] = cc_mail
    message["Subject"] = subject
    #message["Bcc"] = debug_email  # Recommended for mass emails
    message.preamble = "Controllo settimanale anomalie TREG"

    
                    
    # Add body to email
    message.attach(MIMEText(body, "html"))

    for ff in excel_names_array:
        # aggiunto allegato (usando la funzione importata)
        allegato(message, f'{path}/{ff}', ff)
    
    allegato(message, logfile, 'anomalie_QT.log')
    
    #text = message.as_string()

    # Now send or store the message
    logging.info("Richiamo la funzione per inviare mail")
    invio=invio_messaggio(message)
    logging.info(invio) 
    
        
    # check se c_handller contiene almeno una riga 
    error_log_mail(errorfile, 'assterritorio@amiu.genova.it', os.path.basename(__file__), logger)
    
    
    logger.info("chiudo le connessioni in maniera definitiva")
    curr.close()
    conn.close()
    














if __name__ == "__main__":
    main()      