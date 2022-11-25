#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2021
# Roberto Marzocchi

'''
Lo script crea un calendario del servizio per l'UT specificata sulla base dei dati presenti sulla UO

'''

import os, sys, re  # ,shutil,glob
import inspect, os.path

import xlsxwriter
#from xlsxwriter.utility import xl_rowcol_to_cell

#import getopt  # per gestire gli input

#import pymssql

import psycopg2

import cx_Oracle

import datetime
import holidays
from workalendar.europe import Italy


from credenziali import db, port, user, pwd, host, user_mail, pwd_mail, port_mail, smtp_mail


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

import locale
#locale.setlocale(locale.LC_ALL, 'it_IT.UTF-8')
locale.setlocale(locale.LC_TIME, 'it_IT.UTF-8')
import calendar

import csv

#LOG

filename = inspect.getframeinfo(inspect.currentframe()).filename
path     = os.path.dirname(os.path.abspath(filename))

'''#path=os.path.dirname(sys.argv[0]) 
#tmpfolder=tempfile.gettempdir() # get the current temporary directory
logfile='{}/log/variazioni_importazioni.log'.format(path)
#if os.path.exists(logfile):
#    os.remove(logfile)

logging.basicConfig(format='%(asctime)s\t%(levelname)s\t%(message)s',
    filemode='a', # overwrite or append
    filename=logfile,
    level=logging.DEBUG)
'''


path=os.path.dirname(sys.argv[0]) 
#tmpfolder=tempfile.gettempdir() # get the current temporary directory
logfile='{}/log/calendario_servizio.log'.format(path)
errorfile='{}/log/error_calendario_servizio.log'.format(path)
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
    cur = con.cursor()
    
    

    
    
    oggi=datetime.datetime.today()
    oggi=oggi.replace(hour=0, minute=0, second=0, microsecond=0) 
    logger.debug('Oggi {}'.format(oggi))
    
    
    num_giorno=datetime.datetime.today().weekday()
    giorno=datetime.datetime.today().strftime('%A')
    giorno_file=datetime.datetime.today().strftime('%Y%m%d')
    oggi1=datetime.datetime.today().strftime('%d/%m/%Y')
    logger.debug(oggi1)
    #exit()
    #logger.debug('Il giorno della settimana è {} o meglio {}'.format(num_giorno, giorno))
    
    
    first_year=datetime.datetime.strptime('01/01/{}'.format(oggi.year+1), '%d/%m/%Y')
    
    holiday_list = []
    holiday_list_pulita=[]
    for holiday in holidays.Italy(years=[(oggi.year), (oggi.year+1)]).items():
        #print(holiday[0])
        #print(holiday[1])
        holiday_list.append(holiday)
        holiday_list_pulita.append(holiday[0])
    
    
    # AGGIUNGO LA FESTA PATRONALE
    logging.debug('Anno corrente = {}'.format(oggi.year))
    fp = datetime.datetime(oggi.year+1, 6, 24)
    festa_patronale=datetime.date(fp.year, fp.month, fp.day)
    holiday_list_pulita.append(festa_patronale)
    
    
    
    
    
    # PARTI GENERICHE DEL FILE
    
    nome_file="calendario_servizi_{0}.xlsx".format((oggi.year+1))
    file_calendario="{0}/calendario/{1}".format(path,nome_file)
    
    logger.info('Creo nuovo file {}'.format(nome_file))
    workbook = xlsxwriter.Workbook(file_calendario)
    

    date_format = workbook.add_format({'font_size': 9, 'border':   1,
    'num_format': 'dd/mm/yyyy', 'valign': 'vcenter', 'center_across': True})

    date_format_i = workbook.add_format({'font_size': 9, 'border':   1, 'bg_color': '#C5D9F1',
    'num_format': 'dd/mm/yyyy', 'valign': 'vcenter', 'center_across': True})
    
    date_format_e = workbook.add_format({'font_size': 9, 'border':   1, 'bg_color': '#FAC090',
    'num_format': 'dd/mm/yyyy', 'valign': 'vcenter', 'center_across': True})
    

    title = workbook.add_format({'bold': True,  'font_size': 9, 'border':   1, 'bg_color': '#F9FF33', 'valign': 'vcenter', 'center_across': True,'text_wrap': True})
    #text_common 
    #tc_left= 
    tc =  workbook.add_format({'border':   1, 'font_size': 9, 'valign': 'vcenter', 'center_across': True, 'text_wrap': True})
    tc_bold =  workbook.add_format({'bold': True, 'border':   1, 'font_size': 9, 'valign': 'vcenter', 'center_across': True, 'text_wrap': True})
    tc_i = workbook.add_format({'border':   1, 'font_size': 9, 'valign': 'vcenter', 'bg_color': '#C5D9F1', 'center_across': True, 'text_wrap': True})
    tc_e = workbook.add_format({'border':   1, 'font_size': 9, 'valign': 'vcenter', 'bg_color': '#FAC090', 'center_across': True, 'text_wrap': True})

    #D8D8D8
    sunday_tc =  workbook.add_format({'border':   1, 'font_size': 9, 'bg_color': '#D8D8D8',  'valign': 'vcenter',
                                       'center_across': True, 'text_wrap': True})
    holiday_tc =  workbook.add_format({'border':   1, 'font_size': 9, 'bg_color': '#FF8669',  'valign': 'vcenter',
                                       'center_across': True, 'text_wrap': True})
    merge_format = workbook.add_format({
                'bold':     True,
                'border':   1,
                'font_size': 9,
                'align':    'center',
                'valign':   'vcenter',
                'bg_color': '#F9FF33',
                'text_wrap': True
            })
    
    
    merge_legend = workbook.add_format({
                'bold':     True,
                'border':   1,
                'font_size': 9,
                'align':    'center',
                'valign':   'vcenter',
                'text_wrap': True
            })
    
    
    id_uo=[85] # 85 VSOL # 90 val trebbia
    
    
    query=''' SELECT 
        au.DESC_UO AS UO,
        as2.DESC_SERVIZIO AS SERVIZIO,
        CASE
            WHEN count(cpvt.ID_TAPPA) > 0 THEN 'PERCORSO'
            ELSE 'SOLO TESTATA'	
        END
        TIPO,
        aspu.ID_PERCORSO, aspu.DESCRIZIONE, FREQUENZA_NEW AS FREQUENZA, aspu.DURATA, at2.CODICE_TURNO AS TURNO, 
        as3.NUM_ADEC, as3.NUM_AUTI, as3.NUM_ALTRI,
        aspu.DTA_ATTIVAZIONE, aspu.DTA_DISATTIVAZIONE, 
        CASE 
            WHEN as2.FREQUENZE_NON_PROGRAMMATE ='S' OR aspu.FREQUENZA_NEW ='S0000000' THEN 'S'
            ELSE 'N'
        END FREQUENZE_NON_PROGRAMMATE
        FROM ANAGR_SER_PER_UO aspu 
        JOIN ANAGR_UO au ON aspu.ID_UO = au.ID_UO
        JOIN ANAGR_SERVIZI as2 ON as2.ID_SERVIZIO = aspu.ID_SERVIZIO 
        JOIN ANAGR_TURNI at2 ON at2.ID_TURNO = aspu.ID_TURNO
        LEFT JOIN ANAGR_SQUADRE as3 ON as3.ID_SQUADRA = aspu.ID_SQUADRA 
        LEFT JOIN CONS_PERCORSI_VIE_TAPPE cpvt ON cpvt.ID_PERCORSO = aspu.ID_PERCORSO 
        WHERE aspu.DTA_DISATTIVAZIONE > SYSDATE AND aspu.ID_UO = :uo
        GROUP BY au.DESC_UO, as2.DESC_SERVIZIO,
        aspu.ID_PERCORSO, aspu.DESCRIZIONE, FREQUENZA_NEW, aspu.DURATA, at2.CODICE_TURNO, 
        as3.NUM_ADEC, as3.NUM_AUTI, as3.NUM_ALTRI,
        aspu.DTA_ATTIVAZIONE, aspu.DTA_DISATTIVAZIONE, as2.FREQUENZE_NON_PROGRAMMATE
        ORDER BY UO, FREQUENZE_NON_PROGRAMMATE, SERVIZIO '''
    
    
    uu=0
    while uu<len(id_uo):
        try:
            cur.execute(query, (id_uo[uu],))
            cur.rowfactory = makeDictFactory(cur)
            lista_percorsi=cur.fetchall()
        except Exception as e:
            logger.error(query, id_uo[uu])
            logger.error(e)
        
        
        

        
        
        
        
        
            
        #parto a scrivere dalla riga 2
        rr_start=3
        rr=rr_start
        cont=1
        logger.debug(len(lista_percorsi[0]))
        #exit()
        colonna_start= len(lista_percorsi[0]) +6  # sono le colonne della query +6 (colonne con i conteggi)
        colonna_start1=colonna_start
        mm=1
        
        
        # inizializzo calendario prossimo anno
        
        #day_of_year = datetime.date((oggi.year+1), 1, 1)
        end_date = datetime.date((oggi.year+1), 12, 31)
        delta = datetime.timedelta(days=1)
                
        
        
        
        inizio_estate = datetime.date((oggi.year+1), 6, 15)
        fine_estate = datetime.date((oggi.year+1), 9, 14)
        
        
        
        for pp in lista_percorsi:
        
            #logger.debug(pp['UO'])

            # creo il foglio per ogni UT
            if rr == rr_start:
                check_fr=0 # check per cercare frequenze non programmate
                UO = pp['UO']
                w = workbook.add_worksheet(pp['UO'])
                w.merge_range('B1:B2', 'LEGENDA PERCORSI', merge_legend)
                w.write('C1', 'Invernale', tc_i )
                w.write('C2', 'Estivo', tc_e )
                w.merge_range('D1:D2', 'LEGENDA CALENDARIO', merge_legend )
                w.write('E1', 'Festivo', holiday_tc )
                w.write('E2', 'Domenica non festiva', sunday_tc )
                format = workbook.add_format({'border': 1})
                w.conditional_format('B1:E2', {'type': 'no_blanks','format': format})
                w.merge_range('F1:T2', 'Foglio di calcolo ottenuto con i dati presenti su SIT e UO tramite script calendario_servizio.py realizzato dal SIGT (Gestione e Manutenzuione Applicativi)', merge_legend )
                
                
                #imposto la larghezza delle 19 colonne
                w.set_column(0, 0, 0)
                w.set_column(1, 1, 15) #b
                w.set_column(2, 2, 11) # c
                w.set_column(3, 3, 10) # d 
                w.set_column(4, 4, 20) # e
                w.set_column(5, 5, 10) # f
                w.set_column(6, 6, 7) # g h
                w.set_column(7, 7, 6) # g h
                w.set_column(8, 10, 0) # personale
                w.set_column(11, 13, 0)  #
                #w.set_column(8, 9, 12, {'hidden': True})
                
                w.set_column(colonna_start,colonna_start+365,4)
                
                # Hide all rows without data.
                w.set_default_row(hide_unused_rows=True)
                
                # nascondo prima colonna 
                #w.set_column('A:A', None, None, {'hidden': True})
                #w.set_column('I:J', None, None, {'hidden': True})
                
                # creo il calendario
                while mm<(12+1): # ciclo sui mesi
                    colonna_end= colonna_start1-1+calendar.monthrange((oggi.year+1),mm)[1]
                    logger.debug('{0} {1} {2}'.format(calendar.month_name[mm],colonna_start1, colonna_end))
                    w.merge_range(rr_start-3, colonna_start1, rr_start-3, colonna_end, '{}'.format(calendar.month_name[mm]), merge_format)
                    colonna_start1=colonna_end+1
                    mm+=1
                # scrivo i giorni prossimo anno 
                i=0
                day_of_year = datetime.date((oggi.year+1), 1, 1)
                while day_of_year <= end_date:
                    #logger.debug(day_of_year.strftime("%Y/%m/%d"))
                    if day_of_year in holiday_list_pulita:
                        w.write(rr_start-2, colonna_start+i, day_of_year.strftime("%a"), holiday_tc)
                        w.write(rr_start-1, colonna_start+i, day_of_year.strftime("%d"), holiday_tc)
                    elif day_of_year.weekday()==6:
                        w.write(rr_start-2, colonna_start+i, day_of_year.strftime("%a"), sunday_tc)
                        w.write(rr_start-1, colonna_start+i, day_of_year.strftime("%d"), sunday_tc)
                    else :
                        w.write(rr_start-2, colonna_start+i, day_of_year.strftime("%a"), tc)
                        w.write(rr_start-1, colonna_start+i, day_of_year.strftime("%d"), tc)
                    day_of_year=day_of_year+delta
                    i+=1
                   
            if  pp['FREQUENZE_NON_PROGRAMMATE'] == 'S' and check_fr == 0:
                w.merge_range(rr, 0, rr, colonna_start-1, 'ATTENZIONE - I seguenti percorsi non sono programmabili - Le frequenze sono in fase di verifica su UO e comunque da controllare. Il calendario annuale è puramente indicativo', merge_format)
                check_fr=1
                rr+=1
                     
            elif rr > rr_start:
                if UO != pp['UO']:
                    w = workbook.add_worksheet(pp['UO'])
                    '''w.write('B1', 'LEGENDA', tc_bold)
                    w.write('C1', 'Invernale', tc_i )
                    w.write('C2', 'Estivo', tc_e )
                    w.write('D1', 'CALENDARIO', tc )
                    w.write('E1', 'Festivo', holiday_tc )
                    w.write('E2', 'Domenica non festiva', sunday_tc )'''
                    check_fr=0 # check per cercare frequenze non programmate
                    
            

            
            # cerco colonna durata
            col=0         
            for key, value in pp.items():
                if key=='DURATA':
                    #logger.debug(key)
                    col0 = col
                    #logger.debug(col0)
                if key == 'NUM_ADEC':
                    col_adec=col
                if key == 'NUM_AUTI':
                    col_auti=col
                if key == 'NUM_ALTRI':
                    col_altri=col    
                col+=1
            
            
            
                
            
            # TITOLO COLONNA DURATA 
            # totale giorni / # giorni estivi / # giorni invernali
            w.write(rr_start-1,colonna_start-6, 'TOTALE GIORNI (da 380 min)', title)
            w.write(rr_start-1,colonna_start-5, 'GIORNI ESTIVI {} - {}'.format(inizio_estate.strftime('%d/%m'), fine_estate.strftime('%d/%m')), title)
            w.write(rr_start-1,colonna_start-4, 'GIORNI INVERNALI', title)
            w.write(rr_start-1,colonna_start-3, 'TOT ORE ADEC', title)
            w.write(rr_start-1,colonna_start-2, 'TOT ORE AUTI', title)
            w.write(rr_start-1,colonna_start-1, 'TOT ORE ALTRO', title)
            # scrivo i percorsi
            cc=0
            for key, value in pp.items():
                #print(key)
                #print(value)
                
                w.write(rr_start-1, cc, key.replace('_', ' '), title)
                
                #logger.debug(type(value))
                #logger.debug(key)
                #logger.debug(cc)
                # cerco l'indice della colonna dove è indicata la durata
                
                # controllo se percorso stagionale
                query_stag='''SELECT stagionalita, ddmm_switch_on, ddmm_switch_off 
                        FROM elem.percorsi p 
                        WHERE cod_percorso = %s and id_categoria_uso in (3,6) and stagionalita is not null'''
                
                try:
                    curr.execute(query_stag, (pp['ID_PERCORSO'],))
                    stagionali=curr.fetchall()
                except Exception as e:
                    logger.error(query_stag, pp['ID_PERCORSO'])
                    logger.error(e)
                
                
                if len(stagionali)==0:
                    #logger.debug('Percorso annuale')
                    stag = None
                    stile=tc
                    stile_data=date_format
                else:
                    for ss in stagionali:
                        stag=ss[0]
                        s_s=ss[1]
                        s_e=ss[2]
                        date_on= datetime.date(oggi.year+1, int(s_s[2:]), int(s_s[0:2]))
                        date_off=datetime.date(oggi.year+1, int(s_e[2:]), int(s_e[0:2]))
                        if stag=='E':
                            stile=tc_e
                            stile_data=date_format_e
                        elif stag =='I':
                            stile=tc_i
                            stile_data=date_format_i
                if key=='FREQUENZA' or key=='TURNO':
                    w.write(rr, cc, value[0:1], stile)
                else:
                    if type(value) is str:
                        w.write(rr, cc, value, stile)
                    elif type(value) is datetime.datetime :
                        w.write(rr, cc, value, stile_data)
                        #logger.debug(type(value))
                    elif type(value) is int :
                        w.write(rr, cc, value, stile)
                
                #exit()
                
                # calendario
                
                #logger.debug('Sono qua') 
                # scrivo i giorni prossimo anno 
                i=0
                day_of_year = datetime.date((oggi.year+1), 1, 1)
                while day_of_year <= end_date:
                    
                    if day_of_year == inizio_estate:
                        cell3 = xlsxwriter.utility.xl_rowcol_to_cell(rr, colonna_start+i)
                        cell3b = xlsxwriter.utility.xl_rowcol_to_cell(rr, colonna_start+i-1)
                    if day_of_year == fine_estate:
                        cell4 = xlsxwriter.utility.xl_rowcol_to_cell(rr, colonna_start+i)
                        cell4b = xlsxwriter.utility.xl_rowcol_to_cell(rr, colonna_start+i+1)
                    #logger.debug(day_of_year.strftime("%Y/%m/%d"))
                    #logger.debug(day_of_year.weekday())
                    if stag=='E' and (day_of_year < date_on or day_of_year >= date_off) :
                        #logger.debug(date_on)
                        #logger.debug(date_off)
                        #logger.debug('Percorso {} - day_of_year={} --> Scrivo 0'.format(stag, day_of_year))
                        #exit()
                        if day_of_year in holiday_list_pulita:
                            w.write(rr, colonna_start+i, 0, holiday_tc)
                        elif day_of_year.weekday()==6:
                            w.write(rr, colonna_start+i, 0, sunday_tc)
                        else:
                            w.write(rr, colonna_start+i, 0, tc)        
                    elif stag=='I' and (day_of_year >= date_off and day_of_year < date_on) :
                        #logger.debug('Percorso {} - day_of_year={} --> Scrivo 0'.format(stag, day_of_year))
                        if day_of_year in holiday_list_pulita:
                            w.write(rr, colonna_start+i, 0, holiday_tc)
                        elif day_of_year.weekday()==6:
                            w.write(rr, colonna_start+i, 0, sunday_tc)
                        else:
                            w.write(rr, colonna_start+i, 0, tc)
                    else:
                        # CASO 1  frequenza settimanale
                        if pp['FREQUENZA'].startswith('S'):
                            #logger.debug('Caso settimanale')
                            check=pp['FREQUENZA'][day_of_year.weekday()+1:day_of_year.weekday()+2]
                            if day_of_year in holiday_list_pulita:
                                w.write(rr, colonna_start+i, int(check), holiday_tc)
                            elif day_of_year.weekday()==6:
                                w.write(rr, colonna_start+i, int(check), sunday_tc)
                            else:
                                w.write(rr, colonna_start+i, int(check), tc)
                                            
                        # CASO 2 frequenza mensile
                        elif pp['FREQUENZA'].startswith('M'):
                            #logger.debug('Caso mensile')
                            # creo un array facendo lo split
                            giorni=pp['FREQUENZA'][1:].split('_')
                            check_mensile=0
                            k=0
                            while k < len(giorni):
                                #settimana=giorni[k][0:1]
                                #giorno_settimana=giorni[k][1:] # 1=lun / 7=dom
                                if ((int(day_of_year.strftime("%d"))//7+1)==int(giorni[k][0:1]) and (int(giorni[k][1:])-1) == day_of_year.weekday()) : 
                                    #logger.debug('Giorno della settimana è {} - previsto {}.'.format(day_of_year.weekday(), (int(giorni[k][1:])-1)))
                                    check_mensile=1
                                k+=1
                            # scrivo il risultato    
                            if day_of_year in holiday_list_pulita:
                                w.write(rr, colonna_start+i, check_mensile, holiday_tc)
                            elif day_of_year.weekday()==6:
                                w.write(rr, colonna_start+i, check_mensile, sunday_tc)        
                            else:
                                w.write(rr, colonna_start+i, check_mensile, tc)
                                          
                        else:
                            logger.error('Frequenza {} non gestita'.format(pp['FREQUENZA']))
                        
                    day_of_year=day_of_year+delta
                    i+=1
                
                
                
                
                cc+=1
                cell0 = xlsxwriter.utility.xl_rowcol_to_cell(rr, col0)
                cell_adec = xlsxwriter.utility.xl_rowcol_to_cell(rr, col_adec)
                cell_auti = xlsxwriter.utility.xl_rowcol_to_cell(rr, col_auti)
                cell_altri = xlsxwriter.utility.xl_rowcol_to_cell(rr, col_altri)
                cell1 = xlsxwriter.utility.xl_rowcol_to_cell(rr, colonna_start)
                cell2 = xlsxwriter.utility.xl_rowcol_to_cell(rr, colonna_start-1+i)
                #w.write_formula(rr, colonna_start-3, '=SUM({1}:{2})*{0}'.format(cell0, cell1, cell2))
                if check_fr == 0: 
                    w.write_formula(rr, colonna_start-6, '=ROUND(SUM({0}:{1})*{2}/380,1)'.format(cell1, cell2, cell0), tc)
                    w.write_formula(rr, colonna_start-5, '=ROUND(SUM({0}:{1})*{2}/380,1)'.format(cell3, cell4, cell0), tc)
                    w.write_formula(rr, colonna_start-4, '=ROUND((SUM({0}:{1})+SUM({2}:{3}))*{4}/380,1)'.format(cell1, cell3b, cell4b, cell2, cell0), tc)
                    w.write_formula(rr, colonna_start-3, '=ROUND(SUM({0}:{1})*{2}*{3}/60,1)'.format(cell1, cell2, cell0,cell_adec), tc)
                    w.write_formula(rr, colonna_start-2, '=ROUND(SUM({0}:{1})*{2}*{3}/60,1)'.format(cell1, cell2, cell0,cell_auti), tc)
                    w.write_formula(rr, colonna_start-1, '=ROUND(SUM({0}:{1})*{2}*{3}/60,1)'.format(cell1, cell2, cell0,cell_altri), tc)
        
            rr+=1
            cont+=1
            w.autofilter(rr_start-1, 0, rr-1, colonna_start-1+i)  # Same as above.
            #w.set_column(colonna_start+i, None, 0)
        uu+=1
    workbook.close()
    
    cur.close()
    con.close()
    
        # excel lo coloro sulla base di holiday_list_pulita (holiday_tc)
        
        

    
    
    
    
    
if __name__ == "__main__":
    main()