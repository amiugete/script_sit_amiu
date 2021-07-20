#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2021
# Roberto Marzocchi

'''
Script sulla falsariga di quello per gli ecopunti 
non parte da alberghi.base_ecopunti, ma direttamente dai codici via che vanno variati di volta in volta

Esegue le seguenti operazioni:

1) con l'elenco dei codici civici cerca le utenze domestiche e non domestiche su Oracle e produce due file excel

'''


#codici_via= '40500, 19420, 61980, 49860'

file_csv='elenco_vie_test2.txt'
prefisso1='zona valpolcevera'



import os,sys
import inspect, os.path
# da sistemare per Linux
import cx_Oracle


import xlsxwriter


import psycopg2

import datetime
import csv


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

giorno_file='{}_{}'.format(giorno_file, prefisso1.replace(' ', '_'))

logging.basicConfig(
    format='%(asctime)s\t%(levelname)s\t%(message)s',
    filemode ='w',
    #filename='{}\log\{}_{}_ecopunti_parte2.log'.format(path, giorno_file, user),
    level=logging.DEBUG)




def main():
    # carico i mezzi sul DB PostgreSQL
    logging.info('Leggo il file CSV')

    with open(file_csv) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        line_count = 0
        for row in csv_reader:
            if line_count == 0:
                logging.debug(f'Column names are {", ".join(row)}')
                line_count += 1
            elif line_count==1:
                codici_via = '{}'.format(row[0])
                line_count += 1
            else: 
                codici_via = '{}, {}'.format(codici_via, row[0])
                line_count += 1
        logging.debug(f'Processed {line_count-1} lines.')
        logging.debug(codici_via)

    #exit()
    

    logging.info('Connessione al db')
    conn = psycopg2.connect(dbname=db,
                        port=port,
                        user=user,
                        password=pwd,
                        host=host)

    curr = conn.cursor()
    conn.autocommit = True

    
    query='''select n.cod_civico from geo.civici_neri n 
where cod_strada::integer in ({0}) 
union 
select n.cod_civico from geo.civici_rossi n 
where cod_strada::integer in ({0})'''.format(codici_via)
    


    try:
	    curr.execute(query)
	    lista_civici=curr.fetchall()
    except Exception as e:
        logging.error(e)


    #inizializzo gli array
    cod_civico=[]

           
    for vv in lista_civici:
        #logging.debug(vv[0])
        cod_civico.append(vv[0])

    curr.close()


    logging.info('Lista civici')
    curr2 = conn.cursor()
    query2 = ''' SELECT v.nome, be.testo FROM
(select n.testo, n.cod_strada from geo.civici_neri n 
where cod_strada::integer in ({0}) 
union 
select n.testo, n.cod_strada from geo.civici_rossi n 
where cod_strada::integer in ({0})) as be
join  topo.vie v 
ON v.id_via::integer = be.cod_strada::integer'''.format(codici_via)

    try:
	    curr2.execute(query2)
	    lista_civici2=curr2.fetchall()
    except Exception as e:
        logging.error(e)


    
    nome_file0="{0}_elenco_civici_completo.xlsx".format(giorno_file)
    file_civici="{0}/utenze/{1}".format(path,nome_file0)
    
    
    workbook0 = xlsxwriter.Workbook(file_civici)
    w0 = workbook0.add_worksheet()

    w0.write(0, 0, 'id') 
    w0.write(0, 1, 'Nome_via')
    w0.write(0, 2, 'Civico')
    i=1
    for vv in lista_civici2:
        w0.write(i, 0, i) 
        w0.write(i, 1, vv[0])
        w0.write(i, 2, vv[1])
        i+=1
        

    workbook0.close()





    # Array con i civici neri e rossi
    i=0
    k=1
    while i< len(cod_civico):
        if i == 0:
            civ= '''COD_CIVICO IN ('{}' '''.format(cod_civico[i])
        elif i==(k*1000-1):
            k+=1
            civ= '''{} ) OR COD_CIVICO IN ('{}' '''.format(civ, cod_civico[i])
        else:
             civ= '''{} , '{}' '''.format(civ, cod_civico[i])
        i+=1
    civ= ''' {})'''.format(civ)



    # connessione Oracle
    cx_Oracle.init_oracle_client(lib_dir=r"C:\oracle\instantclient_19_10")
    parametri_con='{}/{}@//{}:{}/{}'.format(user_strade,pwd_strade, host_uo,port_uo,service_uo)
    logging.debug(parametri_con)
    con = cx_Oracle.connect(parametri_con)
    logging.info("Versione ORACLE: {}".format(con.version))



    
  



    nome_file="{0}_utenze_domestiche.xlsx".format(giorno_file)
    nome_file2="{0}_utenze_nondomestiche.xlsx".format(giorno_file)
    nome_file3="{0}_civici_utenze_domestiche.xlsx".format(giorno_file)
    nome_file4="{0}_civici_utenze_nondomestiche.xlsx".format(giorno_file)
    file_domestiche="{0}/utenze/{1}".format(path,nome_file)
    file_nondomestiche="{0}/utenze/{1}".format(path,nome_file2)
    file_civdomestiche="{0}/utenze/{1}".format(path,nome_file3)
    file_civnondomestiche="{0}/utenze/{1}".format(path,nome_file4)
    
    workbook = xlsxwriter.Workbook(file_domestiche)
    w = workbook.add_worksheet()



    w.write(0, 0, 'ID_UTENTE') 
    w.write(0, 1, 'PROGR_UTENZA') 
    w.write(0, 2, 'COGNOME') 
    w.write(0, 3, 'NOME')
    w.write(0, 4, 'COD_VIA') 
    w.write(0, 5, 'DESCR_VIA') 
    w.write(0, 6, 'CIVICO') 
    w.write(0, 7, 'LETTERA_CIVICO')
    w.write(0, 8, 'COLORE') 
    w.write(0, 9, 'SCALA') 
    w.write(0, 10, 'PIANO') 
    w.write(0, 11, 'INTERNO')
    w.write(0, 12, 'CAP') 
    w.write(0, 13, 'UNITA_URBANISTICA') 
    w.write(0, 14, 'QUARTIERE') 
    w.write(0, 15, 'CIRCOSCRIZIONE')
    w.write(0, 16, 'ABITAZIONE_DI_RESIDENZA') 
    w.write(0, 17, 'DESCR_CATEGORIA')
    w.write(0, 18, 'DESCR_UTILIZZO') 



    logging.info('*****************************************************')
    logging.info('Utenze domestiche su strade')

    cur = con.cursor()
    query=''' SELECT ID_UTENTE, PROGR_UTENZA, COGNOME, NOME, COD_VIA, DESCR_VIA,
        CIVICO, SUB_CIVICO, COLORE, SCALA, PIANO, INTERNO, CAP, 
        UNITA_URBANISTICA, QUARTIERE, CIRCOSCRIZIONE, ZONA, ABITAZIONE_DI_RESIDENZA,  DESCR_CATEGORIA, DESCR_UTILIZZO
        FROM STRADE.UTENZE_TIA_DOMESTICHE
        WHERE {}
        '''.format(civ)
    

    try: 
        lista_domestiche = cur.execute(query)
    except Exception as e:
        logging.error(query)
        logging.error(e)
        exit()
    

    i=1
    for rr in lista_domestiche:
        j=0
        #logging.debug(len(rr))
        while j<len(rr):
            w.write(i, j, rr[j])
            j+=1
        i+=1

    cur.close()
    workbook.close()



    # civici domestiche
    workbook3 = xlsxwriter.Workbook(file_civdomestiche)
    w3 = workbook3.add_worksheet()

    w3.write(0, 0, 'COD_VIA') 
    w3.write(0, 1, 'DESCR_VIA') 
    w3.write(0, 2, 'CIVICO') 
    w3.write(0, 3, 'LETTERA_CIVICO')
    w3.write(0, 4, 'COLORE')



    cur3 = con.cursor()
    query=''' SELECT DISTINCT COD_VIA, DESCR_VIA,
        CIVICO, SUB_CIVICO, COLORE 
        FROM STRADE.UTENZE_TIA_DOMESTICHE
        WHERE {} ORDER BY DESCR_VIA
        '''.format(civ)

    #logging.debug(query)
    lista_civdomestiche = cur3.execute(query)

    i=1
    for rr in lista_civdomestiche:
        j=0
        #logging.debug(len(rr))
        while j<len(rr):
            w3.write(i, j, rr[j])
            j+=1
        i+=1

    cur3.close()
    workbook3.close()

    logging.info('*****************************************************')
    logging.info('Utenze non domestiche su strade')
    # non domestiche
    cur2 = con.cursor()

    workbook2 = xlsxwriter.Workbook(file_nondomestiche)
    w2 = workbook2.add_worksheet()


    w2.write(0, 0, 'ID_UTENTE') 
    w2.write(0, 1, 'PROGR_UTENZA') 
    w2.write(0, 2, 'NOMINATIVO') 
    w2.write(0, 3, 'CFISC_PARIVA')
    w2.write(0, 4, 'COD_VIA') 
    w2.write(0, 5, 'DESCR_VIA') 
    w2.write(0, 6, 'CIVICO') 
    #w2.write(0, 7, 'SUB_CIVICO')
    w2.write(0, 7, 'COLORE') 
    w2.write(0, 8, 'SCALA') 
    #w2.write(0, 10, 'PIANO') 
    w2.write(0, 9, 'INTERNO')
    w2.write(0, 10, 'LETTERA_INTERNO')
    w2.write(0, 11, 'CAP') 
    w2.write(0, 12, 'UNITA_URBANISTICA') 
    w2.write(0, 13, 'QUARTIERE') 
    w2.write(0, 14, 'CIRCOSCRIZIONE')
    w2.write(0, 15, 'ABITAZIONE_DI_RESIDENZA') 
    w2.write(0, 16, 'DESCR_CATEGORIA')
    w2.write(0, 17, 'DESCR_UTILIZZO')



    query='''SELECT ID_UTENTE, PROGR_UTENZA, NOMINATIVO, CFISC_PARIVA, COD_VIA, DESCR_VIA,
CIVICO, COLORE, SCALA, INTERNO, LETTERA_INTERNO, CAP, 
UNITA_URBANISTICA, QUARTIERE, CIRCOSCRIZIONE, ZONA, SUPERFICIE, NUM_OCCUPANTI, ABITAZIONE_DI_RESIDENZA,  DESCR_CATEGORIA, DESCR_UTILIZZO
FROM STRADE.UTENZE_TIA_NON_DOMESTICHE
WHERE {}
        '''.format(civ)

    lista_nondomestiche = cur2.execute(query)

    i=1
    for rr in lista_nondomestiche:
        j=0
        #logging.debug(len(rr))
        while j<len(rr):
            w2.write(i, j, rr[j])
            j+=1
        i+=1

    cur2.close()
    workbook2.close()



    # civici  non domestiche
    workbook4 = xlsxwriter.Workbook(file_civnondomestiche)
    w4 = workbook4.add_worksheet()

    w4.write(0, 0, 'COD_VIA') 
    w4.write(0, 1, 'DESCR_VIA') 
    w4.write(0, 2, 'CIVICO') 
    w4.write(0, 3, 'LETTERA_CIVICO')
    w4.write(0, 4, 'COLORE')



    cur4 = con.cursor()
    query=''' SELECT DISTINCT COD_VIA, DESCR_VIA,
        CIVICO, LETTERA_CIVICO, COLORE 
        FROM STRADE.UTENZE_TIA_NON_DOMESTICHE
        WHERE {} ORDER BY DESCR_VIA
        '''.format(civ)

    #logging.debug(query)
    lista_civnondomestiche = cur4.execute(query)

    i=1
    for rr in lista_civnondomestiche:
        j=0
        #logging.debug(len(rr))
        while j<len(rr):
            w4.write(i, j, rr[j])
            j+=1
        i+=1

    cur4.close()
    workbook4.close()


    

    exit()

    
    i=0
    #k=1
    cont=0
    logging.info('*****************************************************')
    logging.info('Utenze domestiche su strade')
    while i< len(cod_civico):
        query='''
       SELECT ID_UTENTE, PROGR_UTENZA, COGNOME, NOME, COD_VIA, DESCR_VIA,
CIVICO, SUB_CIVICO, COLORE, SCALA, PIANO, INTERNO, CAP, 
UNITA_URBANISTICA, QUARTIERE, CIRCOSCRIZIONE, ZONA, ABITAZIONE_DI_RESIDENZA,  DESCR_CATEGORIA, DESCR_UTILIZZO
FROM STRADE.UTENZE_TIA_DOMESTICHE
WHERE COD_CIVICO = '{}' '''.format(cod_civico[i])

       
        

        #logging.debug(query)
        lista_domestiche = cur.execute(query)
        #cur.execute('select * from all_tables')
        #k=0
        cc=0
        for r in lista_domestiche:
            #if result[7] < = '':
            j=0
            while j<len(r):
                logging.debug((cont+j+1))
                w.write((cont+cc+1), j, r[j])
                j+=1
            cc+=1
        cont=cont+cc
        i+=1



    cur.close()
    workbook.close()


    cur2 = con.cursor()

    workbook2 = xlsxwriter.Workbook(file_nondomestiche)
    w2 = workbook2.add_worksheet()


    w2.write(0, 0, 'ID_UTENTE') 
    w2.write(0, 1, 'PROGR_UTENZA') 
    w2.write(0, 2, 'NOMINATIVO') 
    w2.write(0, 3, 'CFISC_PARIVA')
    w2.write(0, 4, 'COD_VIA') 
    w2.write(0, 5, 'DESCR_VIA') 
    w2.write(0, 6, 'CIVICO') 
    #w2.write(0, 7, 'SUB_CIVICO')
    w2.write(0, 8, 'COLORE') 
    w2.write(0, 9, 'SCALA') 
    #w2.write(0, 10, 'PIANO') 
    w2.write(0, 11, 'INTERNO')
    w2.write(0, 11, 'LETTERA_INTERNO')
    w2.write(0, 12, 'CAP') 
    w2.write(0, 13, 'UNITA_URBANISTICA') 
    w2.write(0, 14, 'QUARTIERE') 
    w2.write(0, 15, 'CIRCOSCRIZIONE')
    w2.write(0, 16, 'ABITAZIONE_DI_RESIDENZA') 
    w2.write(0, 17, 'DESCR_CATEGORIA')
    w2.write(0, 18, 'DESCR_UTILIZZO') 


    i=0
    #k=1
    cont=0
    logging.info('*****************************************************')
    logging.info('Utenze non domestiche su strade')
    while i< len(cod_civico):
        query='''
       SELECT ID_UTENTE, PROGR_UTENZA, NOMINATIVO, CFISC_PARIVA, COD_VIA, DESCR_VIA,
CIVICO, COLORE, SCALA, INTERNO, LETTERA_INTERNO, CAP, 
UNITA_URBANISTICA, QUARTIERE, CIRCOSCRIZIONE, ZONA, SUPERFICIE, NUM_OCCUPANTI, ABITAZIONE_DI_RESIDENZA,  DESCR_CATEGORIA, DESCR_UTILIZZO
FROM STRADE.UTENZE_TIA_NON_DOMESTICHE
WHERE COD_CIVICO = '{}' '''.format(cod_civico[i])
        #logging.debug(query)
        lista_domestiche = cur2.execute(query)
        #cur.execute('select * from all_tables')
        #k=0
        cc=0
        for r in lista_domestiche:
            #if result[7] < = '':
            j=0
            while j<len(r):
                logging.debug((cont+j+1))
                w2.write((cont+cc+1), j, r[j])
                j+=1
            cc+=1
        cont=cont+cc
        i+=1




        '''except:
            logging.warning('Civico {} non ha utenze domestiche'.format(cod_civico[i]))
        '''
            
        '''w.write((i+1), 1, r[1]) 
        w.write((i+1), 2, r[2]) 
        w.write((i+1), 3, r[3])
        w.write((i+1), 4, r[4]) 
        w.write((i+1), 5, r[5]) 
        w.write((i+1), 6, r[6]) 
        w.write((i+1), 7, r[7])
        w.write((i+1), 8, r'COLORE') 
        w.write((i+1), 9, 'SCALA') 
        w.write((i+1), 10, 'PIANO') 
        w.write((i+1), 11, 'INTERNO')
        w.write((i+1), 12, 'CAP') 
        w.write((i+1), 13, 'UNITA_URBANISTICA') 
        w.write((i+1), 14, 'QUARTIERE') 
        w.write((i+1), 15, 'CIRCOSCRIZIONE')
        w.write((i+1), 16, 'ABITAZIONE_DI_RESIDENZA') 
        w.write((i+1), 17, 'DESCR_CATEGORIA')
        w.write((i+1), 18, 'DESCR_UTILIZZO')''' 
        #k+=1
        i+=1
            
    
    cur2.close()
    workbook2.close()



if __name__ == "__main__":
    main()