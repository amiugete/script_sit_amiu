#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2021
# Roberto Marzocchi

'''
Lo script crea un report settimanale per i percorsi presenti in SIT (funziona per percorsi settimanali che mensili)
Lo scopo è supportare le UT fornendo un cartaceo per quella settimana del percorso in oggetto

Spazzamento / lavaggio 
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


#cerco la directory corrente
currentdir = os.path.dirname(os.path.realpath(__file__))
parentdir = os.path.dirname(currentdir)
sys.path.append(parentdir)

filename = inspect.getframeinfo(inspect.currentframe()).filename

#inizializzo la variabile path
path=currentdir

# nome dello script python
nome=os.path.basename(__file__).replace('.py','')



giorno_file=datetime.datetime.today().strftime('%Y%m%d_%H%M%S')

# inizializzo i nomi dei file di log (per capire cosa stia succedendo)
logfile='{0}/log/{2}_{1}.log'.format(path,nome, giorno_file)
errorfile='{0}/log/{2}_error_{1}.log'.format(path,nome, giorno_file)







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



def sett(giorno):
    if giorno%7==0:
        set=int(giorno/7)
    else:
        set=int(giorno/7)+1
    return set



def ctrl_freq(freq_oggi, freq_prev ):
    if freq_prev[0]=='S':
        '''Frequenza settimanale'''
        check=int(freq_prev[int(freq_oggi[1])])
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


def copy_format(book, fmt):
    '''
    new_format = copy_format(workbook, initial_format)
    '''
    properties = [f[4:] for f in dir(fmt) if f[0:4] == 'set_']
    dft_fmt = book.add_format()
    return book.add_format({k : v for k, v in fmt.__dict__.items() if k in properties and dft_fmt.__dict__[k] != v})



def main(): 
    
    # leggo l'input
    try: 
        #codice='0203009803'
        codice=sys.argv[1]
        logger.info('Inizio creazione report per percorso {}'.format(codice))
    except Exception as e:
        logger.error(e)
        sent_log_by_mail(filename,errorfile)
        exit()
    
    try: 
        #codice='0203009803'
        sempl=sys.argv[2]
        check_s=1
        logger.info('Richiesta report semplificato')
    except Exception as e:
        check_s=0
        logger.info('Report standard')
        
    
        
        
        
        
    
    # verifico se percorso spazzamento o lavaggio
    
    logger.info('Connessione al db')
    conn = psycopg2.connect(dbname=db,
                        port=port,
                        user=user,
                        password=pwd,
                        host=host)

    curr = conn.cursor()
    #conn.autocommit = True
    
    
    query='''select p.cod_percorso, p.versione, p.descrizione, 
case 
	when s.riempimento = 1 then 'racc'
	when s.riempimento = 0 then 'spazz'
	else 'altro'
end tipologia,
s.descrizione as servizio,
u.descrizione as ut,
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
where p.cod_percorso= %s and p.id_categoria_uso in (3,6)
    '''
    
    try:
        curr.execute(query, (codice,))
        dettagli_percorso=curr.fetchall()
    except Exception as e:
        logger.error(e)
        sent_log_by_mail(filename,logfile)
        print('''Manca l'input''')
        exit()


    k=0       
    for dd in dettagli_percorso:
        tipologia=dd[3]
        
    logger.info(tipologia)
    
    curr.close()
    curr = conn.cursor()
   
    
    
    
    

    
    nome_file="report_{0}".format(codice,giorno_file)
    
    if check_s==1:
        nome_file='{}_operatore'.format(nome_file)

    if (os.getuid()==33): # wwww-data
        if not os.path.exists('/tmp/report'):
            os.makedirs("/tmp/report")
        file_report="/tmp/report/{1}.xlsx".format(path,nome_file)
    else:
        file_report="{0}/report/{1}.xlsx".format(path,nome_file)
    
    
    workbook = xlsxwriter.Workbook(file_report)
    w = workbook.add_worksheet()

    cell_format = workbook.add_format()
    cell_format.set_bold(True)
    cell_format.set_border(1)
    
    

    cell_format_title = workbook.add_format()
    cell_format_title.set_border(1)
    cell_format_title.set_bold(True)
    cell_format_title.set_font_color('#144798')


    cell_format_colorato = copy_format(workbook, cell_format)
    cell_format_colorato.set_bg_color('#3CFF1A')
    cell_format_colorato.set_align('center_across')


    cell_format_grande = copy_format(workbook, cell_format)
    cell_format_grande.set_font_size(13)
    cell_format_grande.set_bg_color('yellow')
    
    cell_format_title_grande = copy_format(workbook, cell_format_title)
    cell_format_title_grande.set_font_size(13)
    
       
    merge_format = workbook.add_format({
    'border': 1,
    'align': 'center',
    'valign': 'vcenter'})
    
    merge_format_grande=copy_format(workbook, merge_format)
    merge_format_grande.set_font_size(13)    
    merge_format_grande.set_bg_color('yellow')

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
    w.set_footer('&RReport prodotto grazie a SW del gruppo APTE (assterritorio@amiu.genova.it) in data &D alle ore &T')
    w.fit_to_pages(1, 0) # 1 page wide and as long as necessary.


    w.insert_image('I2', '{}/img/logo_amiu.jpg'.format(path), {'x_scale': 0.8, 'y_scale': 0.8, 'x_offset': 10, 'y_offset': 10})

    if tipologia=='racc' or tipologia=='altro':
        w.set_column(0,0, 6)
        w.set_column(1,1, 34)
        w.set_column(2,2, 40)
        w.set_column(3,3, 30)
        w.set_column(4,4, 30)
        w.set_column(5,11, 6.5)
    elif tipologia=='spazz':
        if check_s==0:
            w.set_column(0,0, 30)
            w.set_column(1,1, 60)
            w.set_column(2,2, 15)
            w.set_column(3,3, 15)
        else:
            w.set_column(0,0, 40)
            w.set_column(1,1, 80)
            w.set_column(2,2, 0)
            w.set_column(3,3, 0)
        w.set_column(4,4, 20)
        w.set_column(5,11, 6.5)


    w.write('A1', 'codice', cell_format_title) 
    if check_s==0:
        w.write('C1', 'Vers', cell_format_title) 
        w.write('C2', 'Turno', cell_format_title) 
        w.write('C3', 'Stagion.', cell_format_title) 
        w.write('C4', 'Data ultima mod', cell_format_title) 
    
    w.write('A2', 'Serv', cell_format_title)     
    w.write('A3', 'Mezzo', cell_format_title) 
    w.write('A4', 'UT', cell_format_title) 
    w.write('C4', 'Frequenza', cell_format_title) 



    w.write(0, 4, 'Descrizione', cell_format_title) 
    


    k=0       
    for dd in dettagli_percorso:
        w.write('B1', dd[0], cell_format_grande) # codice percorso
        if check_s==0: 
            w.write('D1', dd[1], cell_format) # versione
            w.write('D2', dd[6], cell_format) # turno
            w.write('D3', dd[8], cell_format) # stagionalita
            w.merge_range('F2:H2', dd[10], data_format) # data modifica

        w.write('B2', dd[4], cell_format) # tipo servizio
        w.write('B3', dd[7], cell_format) # mezzo
        w.write('B4', dd[5], cell_format) # ut 
        
        #w.write(3, 3, dd[9], cell_format) # frequenza (spostata sotto)

        w.merge_range('D4:H4', dd[9], merge_format) # frequenza
        w.merge_range('F1:L1', dd[2], merge_format_grande) # descrizione percorso
        
        
        
        
        
            
    if tipologia=='racc' or tipologia=='altro' :
        w.write(5,0, 'PdR', cell_format_title)
        w.write(5,1, 'Indirizzo', cell_format_title)
        w.write(5,2, 'Riferimento', cell_format_title)
        w.write(5,3, 'Tipologia',cell_format_title)
        w.write(5,4, 'Note', cell_format_title)


        


        query_elementi= ''' select coalesce(e.id_piazzola, e.id_elemento) as id, 
        concat(v.nome, ', ',
        coalesce(e.numero_civico, p.numero_civico)) as indirizzo, 
        coalesce(e.riferimento, p.riferimento) as riferimento, 
        te.descrizione, 
        coalesce(p.note,e.note) as note,
        eap.frequenza, fo.descrizione_long, fo.freq_binaria,
        count(e.id_elemento) 
        from elem.elementi_aste_percorso eap
        join elem.aste_percorso ap
        on eap.id_asta_percorso = ap.id_asta_percorso 
        join elem.elementi e 
        on eap.id_elemento = e.id_elemento 
        left join elem.piazzole p 
        on p.id_piazzola = e.id_piazzola
        join elem.aste a
        on e.id_asta = a.id_asta 
        join topo.vie v 
        on v.id_via = a.id_via 
        join etl.frequenze_ok fo 
        on eap.frequenza::int = fo.cod_frequenza  
        join elem.tipi_elemento te 
        on te.tipo_elemento = e.tipo_elemento 
        where id_percorso = (select id_percorso from elem.percorsi p where p.cod_percorso= %s and id_categoria_uso in (3,6))  
        group by coalesce(e.id_piazzola, e.id_elemento) , 
        concat(v.nome, ', ',
        coalesce(e.numero_civico, p.numero_civico)) , 
        coalesce(e.riferimento, p.riferimento), 
        te.descrizione, 
        coalesce(p.note,e.note),
        eap.frequenza, fo.descrizione_long, fo.freq_binaria, ap.num_seq
        order by ap.num_seq asc'''


        try:
            curr.execute(query_elementi, (codice,))
            lista_elementi=curr.fetchall()
        except Exception as e:
            logger.error(e)
            sent_log_by_mail(filename,logfile)


        k=0       
        for vv in lista_elementi:
            w.write(6+k,0, vv[0], cell_format) # id
            w.write(6+k,1, vv[1], cell_format) # indirizzo
            w.write(6+k,2, vv[2], cell_format) # riferimento
            w.write(6+k,3, '{} x {}'.format(vv[8],vv[3]), cell_format) # tipo
            w.write(6+k,4, vv[4], cell_format) # note
            i=0
            while i<7:
                c=i-datetime.datetime.today().weekday()
                giorno = datetime.datetime.today()+datetime.timedelta(days=c)
                g2=giorno.weekday()+1
                g1=sett(giorno.day)
                w.write(5,5+i, '{} ({}s)'.format(dayNameFromWeekday(giorno.weekday()), g1), cell_format_title)
                stringa='{}{}'.format(g1, g2)
                logger.debug(stringa)
                logger.debug(vv[7])
                logger.debug(ctrl_freq(stringa,vv[7]))
                if ctrl_freq(stringa,vv[7])==1:
                    if check_s==1:
                        w.write(6+k,5+i, 'x', cell_format_colorato)
                    else:
                        w.write(6+k,5+i, 'x', cell_format)
                else: 
                    w.write(6+k,5+i, None, cell_format)
                i+=1
            k+=1
        
        if k==0:
            logger.error("Percorso non presente su SIT")
            sys.exit("Percorso non presente su SIT")
    elif tipologia=='spazz':
        
        w.write(5,0, 'Via', cell_format_title)
        if check_s==0:
            w.write(5,1, 'Nota via', cell_format_title)
            w.write(5,2, 'Metri lineari', cell_format_title)
            w.write(5,3, 'Metri quadrati',cell_format_title)
            w.write(5,4, 'Frequenza', cell_format_title)
        else: 
            w.merge_range('B6:E6', 'Nota via', cell_format_title)


        


        query_aste= ''' select   v.nome, ap.nota, ap.frequenza, fo.descrizione_long, fo.freq_binaria, 
sum(a.lung_asta) as ml, 
sum(a.lung_asta*a.larg_asta) as mq,
sum(
case 
	WHEN ap.lato_servizio = 'destro'::text THEN mq.area_d
	WHEN ap.lato_servizio = 'sinistro'::text THEN mq.area_p
	    ELSE mq.area
	END * (ap.lung_trattamento / 100::numeric)
) as mq_ok
 from elem.aste_percorso ap
 --on eap.id_asta_percorso = ap.id_asta_percorso 
join elem.aste a
on ap.id_asta = a.id_asta
left JOIN elem.v_aste_mq mq ON mq.id_asta = ap.id_asta
join topo.vie v 
on v.id_via = a.id_via 
join etl.frequenze_ok fo 
on ap.frequenza::int = fo.cod_frequenza  
where ap.id_percorso = (select id_percorso from elem.percorsi p where p.cod_percorso= %s and p.id_categoria_uso in (3,6))  
group  by v.nome, ap.nota, ap.frequenza, fo.descrizione_long, fo.freq_binaria
order by min(ap.num_seq) asc'''


        try:
            curr.execute(query_aste, (codice,))
            lista_aste=curr.fetchall()
        except Exception as e:
            logger.error(e)
            sent_log_by_mail(filename,logfile)


        #inizializzo le somme
        mq_lun=0
        mq_mar=0
        mq_mer=0
        mq_gio=0
        mq_ven=0
        mq_sab=0
        mq_dom=0
        
        k=0       
        for vv in lista_aste:
            w.write(6+k,0, vv[0], cell_format)
            if check_s==0:
                w.write(6+k,1, vv[1], cell_format)
                w.write(6+k,2, vv[5], cell_format)
                w.write(6+k,3, round(vv[7]), cell_format)
                w.write(6+k,4, vv[3], cell_format)
            else:
                w.merge_range(6+k, 1, 6+k, 4, vv[1], cell_format)
            i=0
            while i<7:
                c=i-datetime.datetime.today().weekday()
                giorno = datetime.datetime.today()+datetime.timedelta(days=c)
                g2=giorno.weekday()+1
                g1=sett(giorno.day)
                w.write(5,5+i, '{} ({}s)'.format(dayNameFromWeekday(giorno.weekday()), g1), cell_format_title)
                stringa='{}{}'.format(g1, g2)
                logger.debug(stringa)
                logger.debug(vv[4])
                logger.debug(ctrl_freq(stringa,vv[4]))
                if ctrl_freq(stringa,vv[4])==1:
                    if check_s == 0:
                        w.write(6+k,5+i, 'x', cell_format)
                    else: 
                        w.write(6+k,5+i, 'x', cell_format_colorato)
                    #w.write(6+k,5+i, '{} mq'.format(vv[6]), cell_format)
                    # faccio sommatoria per giorno
                    if i==0:
                        mq_lun+=vv[7]
                    elif i==1:
                        mq_mar+=vv[7]
                    elif i==2:
                        mq_mer+=vv[7]
                    elif i==3:
                        mq_gio+=vv[7]
                    elif i==4:
                        mq_ven+=vv[7]
                    elif i==5:
                        mq_sab+=vv[7]
                    elif i==6:
                        mq_dom+=vv[7]
                else: 
                    w.write(6+k,5+i, None, cell_format)
                i+=1
            k+=1
        if check_s == 0:    
            if mq_lun>0:
                w.write(6+k,5, '{} mq'.format(round(mq_lun)), cell_format_title)
            if mq_mar>0:
                w.write(6+k,6, '{} mq'.format(round(mq_mar)), cell_format_title)
            if mq_mer>0:
                w.write(6+k,7, '{} mq'.format(round(mq_mer)), cell_format_title)
            if mq_gio>0:
                w.write(6+k,8, '{} mq'.format(round(mq_gio)), cell_format_title)
            if mq_ven>0:
                w.write(6+k,9, '{} mq'.format(round(mq_ven)), cell_format_title)
            if mq_sab>0:
                w.write(6+k,10, '{} mq'.format(round(mq_sab)), cell_format_title)
            if mq_dom>0:
                w.write(6+k,11, '{} mq'.format(round(mq_dom)), cell_format_title)
        
        
        
        if k==0:
            logger.error("Percorso non presente su SIT")
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