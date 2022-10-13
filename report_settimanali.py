#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2021
# Roberto Marzocchi

'''
Lo script crea un report settimanale per i percorsi presenti in SIT (funziona per percorsi settimanali che mensili)
Lo scopo è supportare le UT fornendo un cartaceo per quella settimana del percorso in oggetto 
'''

import os, sys, re  # ,shutil,glob
import inspect, os.path

import psycopg2

import xlsxwriter

import datetime

from credenziali import *

from mail_log import *

#import requests

import logging



def sett(giorno):
    if giorno%7==0:
        set=int(giorno/7)
    else:
        set=int(giorno/7)+1
    return set



def ctrl_freq(freq_oggi, freq_prev ):
    if freq_prev[0]=='S':
        '''Frequenza settimanale'''
        check=freq_prev[freq_oggi[1]]
    elif freq_prev[0]=='M':
        '''Frequenza mensile'''
        if freq_prev.find(freq_oggi)>0:
            check=1
        else:
            check=0    
    return check


def dayNameFromWeekday(weekday):
    if weekday == 0:
        return "LU"
    if weekday == 1:
        return "MA"
    if weekday == 2:
        return "ME"
    if weekday == 3:
        return "GI"
    if weekday == 4:
        return "VE"
    if weekday == 5:
        return "SA"
    if weekday == 6:
        return "DO"





def main(): 
    filename = inspect.getframeinfo(inspect.currentframe()).filename
    path     = os.path.dirname(os.path.abspath(filename))

    #path=os.path.dirname(sys.argv[0]) 
    #tmpfolder=tempfile.gettempdir() # get the current temporary directory
    logfile='{}/log/print_report.log'.format(path)
    #if os.path.exists(logfile):
    #    os.remove(logfile)

    logging.basicConfig(
        handlers=[logging.FileHandler(filename=logfile, encoding='utf-8', mode='w')],
        format='%(asctime)s\t%(levelname)s\t%(message)s',
        #filemode='w', # overwrite or append
        #fileencoding='utf-8',
        #filename=logfile,
        level=logging.INFO)

    try: 
        #codice='0203009803'
        codice=sys.argv[1]
        logging.info('Inizio creazione report per percorso {}'.format(codice))
    except Exception as e:
        logging.error(e)
        sent_log_by_mail(filename,logfile)

    

    nome_file="report_{}.xlsx".format(codice)
    file_report="{0}/report/{1}".format(path,nome_file)
    
    
    workbook = xlsxwriter.Workbook(file_report)
    w = workbook.add_worksheet()

    cell_format = workbook.add_format()
    
    cell_format.set_border(1)

    cell_format_title = workbook.add_format()
    cell_format_title.set_border(1)
    cell_format_title.set_bold(True)
    cell_format_title.set_font_color('#144798')

   
    merge_format = workbook.add_format({
    'border': 1,
    'align': 'center',
    'valign': 'vcenter'})

    data_format = workbook.add_format({
        'num_format': 'dd/mm/yyyy hh:mm', 
        'border': 1,
        'align': 'center'
    })



    # PAGE SETUP
    w.set_landscape()
    w.set_paper(9) #A4
    w.center_vertically()
    #w.set_margins(left, right, top=0.5, bottom=1.5)
    w.repeat_rows(6)
    w.set_header('&CPage &P of &N')
    w.set_footer('&RReport prodotto grazie a SW del gruppo GETE in data &D alle ore &T')
    w.fit_to_pages(1, 0) # 1 page wide and as long as necessary.


    w.insert_image('I2', '{}/img/logo_amiu.jpg'.format(path), {'x_scale': 0.8, 'y_scale': 0.8, 'x_offset': 10, 'y_offset': 10})


    w.set_column(0,0, 30)
    w.set_column(1,1, 10)
    w.set_column(2,2, 40)
    w.set_column(3,3, 30)
    w.set_column(4,4, 30)
    w.set_column(5,11, 6.5)


    w.write(0, 0, 'cod_percorso', cell_format_title) 
    w.write(1, 0, 'Versione', cell_format_title) 
    w.write(2, 0, 'Turno', cell_format_title) 
    w.write(3, 0, 'Stagionalità', cell_format_title) 

    w.write(0, 2, 'Tipo Servizio', cell_format_title) 
    w.write(1, 2, 'Mezzo', cell_format_title) 
    w.write(2, 2, 'UT', cell_format_title) 
    w.write(3, 2, 'Frequenza', cell_format_title) 


    w.write(0, 4, 'Descrizione', cell_format_title) 
    w.write(1, 4, 'Data ultima mod', cell_format_title) 


    w.write(5,0, 'Via', cell_format_title)
    w.write(5,1, 'Civico', cell_format_title)
    w.write(5,2, 'Riferimento', cell_format_title)
    w.write(5,3, 'Tipologia',cell_format_title)
    w.write(5,4, 'Note', cell_format_title)


    logging.info('Connessione al db')
    conn = psycopg2.connect(dbname=db,
                        port=port,
                        user=user,
                        password=pwd,
                        host=host)

    curr = conn.cursor()
    conn.autocommit = True

    

    query_intestazione='''select p.cod_percorso, p.versione, p.descrizione, 
s.descrizione as servizio, u.descrizione as ut,
t.descrizione as turno, 
a.nome as mezzo, p.stagionalita,
fo.descrizione_long,
data_attivazione 
from elem.percorsi p 
join elem.servizi s 
on p.id_servizio =s.id_servizio 
join elem.percorsi_ut pu 
on pu.cod_percorso =p.cod_percorso 
join topo.ut u 
on u.id_ut = pu.id_ut 
join elem.turni t 
on t.id_turno = p.id_turno 
join elem.automezzi a 
on a.cdaog3 = p.famiglia_mezzo 
left join etl.frequenze_ok fo 
on fo.cod_frequenza = p.frequenza 
where p.cod_percorso= %s'''



    try:
        curr.execute(query_intestazione, (codice,))
        dettagli_percorso=curr.fetchall()
    except Exception as e:
        logging.error(e)
        sent_log_by_mail(filename,logfile)


    k=0       
    for dd in dettagli_percorso:
        w.write(0, 1, dd[0], cell_format) 
        w.write(1, 1, dd[1], cell_format) 
        w.write(2, 1, dd[5], cell_format) 
        w.write(3, 1, dd[7], cell_format) 

        w.write(0, 3, dd[3], cell_format) 
        w.write(1, 3, dd[6], cell_format) 
        w.write(2, 3, dd[4], cell_format) 
        w.write(3, 3, dd[8], cell_format) 


        w.merge_range('F1:L1', dd[2], merge_format) 
        w.merge_range('F2:H2', dd[9], data_format) 


    query_elementi= ''' select v.nome, e.numero_civico, e.riferimento, te.descrizione , ap.nota, eap.frequenza, fo.descrizione_long, fo.freq_binaria 
 from elem.elementi_aste_percorso eap
 join elem.aste_percorso ap
 on eap.id_asta_percorso = ap.id_asta_percorso 
join elem.elementi e 
on eap.id_elemento = e.id_elemento 
join elem.aste a
on e.id_asta = a.id_asta 
join topo.vie v 
on v.id_via = a.id_via 
join etl.frequenze_ok fo 
on eap.frequenza::int = fo.cod_frequenza  
join elem.tipi_elemento te 
on te.tipo_elemento = e.tipo_elemento 
where id_percorso = (select id_percorso from elem.percorsi p where p.cod_percorso= %s and id_categoria_uso=3)  
order by ap.num_seq asc'''


    try:
        curr.execute(query_elementi, (codice,))
        lista_elementi=curr.fetchall()
    except Exception as e:
        logging.error(e)
        sent_log_by_mail(filename,logfile)


    k=0       
    for vv in lista_elementi:
        w.write(6+k,0, vv[0], cell_format)
        w.write(6+k,1, vv[1], cell_format)
        w.write(6+k,2, vv[2], cell_format)
        w.write(6+k,3, vv[3], cell_format)
        w.write(6+k,4, vv[4], cell_format)
        i=0
        while i<7:
            c=i-datetime.datetime.today().weekday()
            giorno = datetime.datetime.today()+datetime.timedelta(days=c)
            g2=giorno.weekday()+1
            g1=sett(giorno.day)
            w.write(5,5+i, '{} ({}s)'.format(dayNameFromWeekday(giorno.weekday()), g1), cell_format_title)
            stringa='{}{}'.format(g1, g2)
            logging.debug(stringa)
            logging.debug(vv[7])
            logging.debug(ctrl_freq(stringa,vv[7]))
            if ctrl_freq(stringa,vv[7])==1:
                w.write(6+k,5+i, 'x', cell_format)
            else: 
                w.write(6+k,5+i, None, cell_format)
            i+=1
        k+=1
    
    if k==0:
        logging.error("Percorso non presente su SIT")
        sys.exit("Percorso non presente su SIT")








    workbook.close()
    #sent_log_by_mail(filename,logfile)

    

    '''
    if (weekday == 0 and day in (1, 2, 3)) or (weekday in (1, 2, 3, 4) and day == 1):
        print("Today {0} is the first weekday of the month.".format(date_str))
    else:
        print("Today {0} is not the first weekday of the month.".format(date_str))
    '''


    '''
    import win32com.client as win32
    excel = win32.gencache.EnsureDispatch('Excel.Application')
    wb = excel.Workbooks.Open(file_report)
    ws = wb.Worksheets("Sheet1")
    ws.Columns.AutoFit()
    wb.Save()
    excel.Application.Quit()
    '''

if __name__ == "__main__":
    main()