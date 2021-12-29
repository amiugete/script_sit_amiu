#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2021
# Roberto Marzocchi

'''
Script per esportare su excel l'elenco delle vie su cui controllare la transitabilità del grafo
'''


import os,sys, getopt
import inspect, os.path
# da sistemare per Linux
import cx_Oracle


import xlsxwriter


import psycopg2

import datetime

from urllib.request import urlopen
import urllib.parse

currentdir = os.path.dirname(os.path.realpath(__file__))
parentdir = os.path.dirname(currentdir)
sys.path.append(parentdir)

from credenziali import *
#from credenziali import db, port, user, pwd, host, user_mail, pwd_mail, port_mail, smtp_mail



#libreria per gestione log
import logging


#num_giorno=datetime.datetime.today().weekday()
#giorno=datetime.datetime.today().strftime('%A')

filename = inspect.getframeinfo(inspect.currentframe()).filename
path     = os.path.dirname(os.path.abspath(filename))


giorno_file=datetime.datetime.today().strftime('%Y%m%d')


logfile='{}/log/{}correzioni_grafo.log'.format(path, giorno_file)

logging.basicConfig(
    handlers=[logging.FileHandler(filename=logfile, encoding='utf-8', mode='a')],
    format='%(asctime)s\t%(levelname)s\t%(message)s',
    #filemode='w', # overwrite or append
    #fileencoding='utf-8',
    #filename=logfile,
    level=logging.INFO)




def main():




    # carico i mezzi sul DB PostgreSQL
    logging.info('Connessione al db')
    conn = psycopg2.connect(dbname=db,
                        port=port,
                        user=user,
                        password=pwd,
                        host=host)

    curr = conn.cursor()
    conn.autocommit = True
    query='''select id_zona, descrizione from topo.ut where id_zona in (1,2,3) order by 1'''
    
    try:
        curr.execute(query)
        lista_ut=curr.fetchall()
    except Exception as e:
        logging.error(e)


    #inizializzo gli array
    #ut=[]

           
    for u in lista_ut:
        #logging.debug(vv[0])
        #ut.append(vv[0])
        zona=u[0]
        ut=u[1]

        query2='''select distinct classificazione, id_asta, piazzole, nome, max_trans_percorsi, trans_grafo
            from marzocchir.v_grafo_incongruenze vgi 
            where ut='{}' order by 4'''.format(ut)
        curr2= conn.cursor()



        file_ut="{0}/correzioni_grafo/{1}_{2}.xlsx".format(path,zona,ut.replace(' ', '_'))
        workbook = xlsxwriter.Workbook(file_ut)
        w1 = workbook.add_worksheet('Aste sovradimensionate')
        w2 = workbook.add_worksheet('Aste sottodimensionate')

        w1.set_tab_color('green')
        w2.set_tab_color('red')

        # Add a format. Light red fill with dark red text.
        format1 = workbook.add_format({'bg_color': '#FFC7CE',
                                    'font_color': '#9C0006'})

        # Add a format. Green fill with dark green text.
        format2 = workbook.add_format({'bg_color': '#C6EFCE',
                                    'font_color': '#006100'})

        title = workbook.add_format({'bold': True, 'bg_color': '#F9FF33', 'valign': 'vcenter', 'center_across': True,'text_wrap': True})
        text = workbook.add_format({'text_wrap': True})

        w1.set_column(0, 0, 15)
        w2.set_column(0, 0, 15)
        w1.set_column(1, 4, 50)
        w2.set_column(1, 4, 50)
        w1.set_column(5, 6, 70)
        w2.set_column(5, 6, 70)

        w1.set_row(0, 80)
        w2.set_row(0, 80)

        bold = workbook.add_format({'bold': True})
        


        w1.write(0, 0, 'id_asta', title) 
        w1.write(0, 1, 'piazzole', title) 
        w1.write(0, 2, 'nome_via', title) 
        w1.write(0, 3, 'max_trans_percorsi', title) 
        w1.write(0, 4, 'trans_grafo', title)
        w1.write(0, 5, '''correzione
            - indicare la corretta transitabilità del grafo se è necessario ridurla,
            - lasciare vuoto se la transitabilità del grafo è corretta
            ''', title) 
        w1.write(0, 6, '''correzione
            - indicare la corretta transitabilità delle piazzole se è necessario ridurla,
            - lasciare vuoto se la transitabilità delle piazzole è corretta
            ''', title) 
        w1.write(0, 7, '''eventuali note
            SPECIFICARE NELLE NOTE EVENTUALI CASI PARTICOLARI 
            (es. postazione interna a XXX dove ci possiamo entrare solo con mezzo XXX)''', title)   
        
        w2.write(0, 0, 'id_asta', title) 
        w2.write(0, 1, 'piazzole', title) 
        w2.write(0, 2, 'nome_via', title)
        w2.write(0, 3, 'max_trans_percorsi', title) 
        w2.write(0, 4, 'trans_grafo', title)
        w2.write(0, 5, '''correzione asta
            - lasciare vuoto se è corretto il mezzo che ci sta passando,
            - indicare la corretta transitabilità qualora ci sia un errore.''', title)
        w2.write(0, 6, '''correzione piazzola
            - lasciare vuoto se è corretto il mezzo che ci sta passando,
            - indicare la corretta transitabilità qualora ci sia un errore.''', title)      
        w2.write(0, 7, 'eventuali note', title)          

    
        try:
            curr2.execute(query2)
            lista_vie=curr2.fetchall()
        except Exception as e:
            logging.error(e)

        s=1
        S=1
        for vv in lista_vie:
            if vv[0]=='grafo_sottostimato':
                j=1
                while j<len(vv):
                    w2.write(s, j-1, vv[j], text)
                    j+=1
                s+=1
            elif vv[0]=='grafo_sovrastimato':
                j=1
                while j<len(vv):
                    w1.write(S, j-1, vv[j], text)
                    j+=1
                S+=1     

        workbook.close()


    #query_lizmap=''
        
    curr.close()
    curr2.close()

    url_map='https://amiugis.amiu.genova.it/mappe/lizmap/www/index.php/view/map/'
    repository='repo1'
    project='transitabilita_grafo'
    epsg=3857
    crs='EPSG:{}'.format(epsg)

    query3 = '''select u.id_ut, cuz.id, u.descrizione, 
    replace(replace(replace(st_extent(st_transform(geoloc,{}))::text,'BOX(',''),')',''),' ',',') from geo.confini_ut_zona cuz
    join topo.ut u 
    on u.descrizione = cuz.descrizione 
    group by u.id_ut,cuz.id, u.descrizione'''.format(epsg)

    print(query3)
    curr3 = conn.cursor()


    try:
        curr3.execute(query3)
        lista_ut2=curr3.fetchall()
    except Exception as e:
        logging.error(e)

    for uu in lista_ut2:

        params={ 'repository' : repository,
                    'project': project,
                    #'layers': 'B000TTTTT',
                    'bbox': uu[3],
                    'crs':crs,
                    'filter': '''v_grafo_incongruenze:UT+:+"id"+IN+(+{}+)'''.format(uu[1])
                    }
                
        url_2 = urllib.parse.urlencode(params)
        url_ok = '{}?{}'.format(url_map, url_2)
        print(uu[0])
        print(url_ok)

    curr.close()





if __name__ == "__main__":
    main()   