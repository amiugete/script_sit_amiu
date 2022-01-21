#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2021
# Roberto Marzocchi

'''
Lo script legge i turni attivi su U.O. ed effettua l'allineamento con SIT:
1. verifica se il turno esiste 
caso A in SIT il turno esiste e id è corretto allinea solo la descrizione
caso B in SIT il turno esiste, ma id non è corretto, per cui occorre aggiornare i turni con il vecchio id e usare quello nuovo
caso C in SIT il turno non esiste e lo crea 


Quindi allinea i turni di tutti i percorsi da U.O. a SIT
'''

from msilib import type_short
import os, sys, re  # ,shutil,glob

#import getopt  # per gestire gli input

#import pymssql


import xlsxwriter

import psycopg2

import cx_Oracle

currentdir = os.path.dirname(os.path.realpath(__file__))
parentdir = os.path.dirname(currentdir)
sys.path.append(parentdir)
from credenziali import *


#import requests

import logging

path=os.path.dirname(sys.argv[0]) 
#tmpfolder=tempfile.gettempdir() # get the current temporary directory
logfile='{}/log/turni.log'.format(path)
#if os.path.exists(logfile):
#    os.remove(logfile)

logging.basicConfig(
    handlers=[logging.FileHandler(filename=logfile, encoding='utf-8', mode='w')],
    format='%(asctime)s\t%(levelname)s\t%(message)s',
    #filemode='w', # overwrite or append
    #fileencoding='utf-8',
    #filename=logfile,
    level=logging.INFO)



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
from allegato_mail import *


def main():
    # Mi connetto al DB oracle
    cx_Oracle.init_oracle_client("C:\oracle\instantclient_19_10") # necessario configurare il client oracle correttamente
    #cx_Oracle.init_oracle_client() # necessario configurare il client oracle correttamente
    parametri_con='{}/{}@//{}:{}/{}'.format(user_uo,pwd_uo, host_uo,port_uo,service_uo)
    logging.debug(parametri_con)
    con = cx_Oracle.connect(parametri_con)
    logging.info("Versione ORACLE: {}".format(con.version))
    cur = con.cursor()


    nome_db=db #db_test
    
    # carico i mezzi sul DB PostgreSQL
    logging.info('Connessione al db {}'.format(nome_db))
    conn = psycopg2.connect(dbname=nome_db,
                        port=port,
                        user=user,
                        password=pwd,
                        host=host)

    curr = conn.cursor()
    conn.autocommit = True


    
    id_uo=[]
    h_s=[]
    h_e=[]
    m_s=[]
    m_e=[]
    codice=[]
    fascia=[]
    desc=[]

    query='''SELECT
    ID_TURNO, CODICE_TURNO, DESCRIZIONE, FASCIA_TURNO, DESCR_ORARIO,
    INIZIO_ORA, FINE_ORA, INIZIO_MINUTI, FINE_MINUTI
    FROM ANAGR_TURNI at2 
    WHERE DTA_DISATTIVAZIONE > SYSDATE 
    ORDER BY ID_TURNO '''
    try:
        cur.execute(query)
        lista_turni=cur.fetchall()
    except Exception as e:
        logging.error(query)
        logging.error(e)
    cur.close()

    logging.info('leggo i turni attivi di UO')
    for t_uo in lista_turni:
        #logging.debug('Id_turno (uo): {} '.format(t_uo[0]))
        #logging.debug('Tupla di {} '.format(len(t_uo)))
        id_uo.append(int(t_uo[0]))
        desc.append(t_uo[4])
        h_s.append(t_uo[5])
        h_e.append(t_uo[6])
        m_s.append(t_uo[7])
        m_e.append(t_uo[8])
        codice.append(t_uo[1])
        fascia.append(t_uo[3])

    # query_pg='''select id_turno, descrizione, cod_turno 
    # from id_turnoelem.turni t 
    # where inizio_ora ={0}
    # and fine_ora = {1}
    # and inizio_minuti={2}
    # and fine_minuti= {3} '''.format(t_uo[5], t_uo[6], t_uo[7], t_uo[8])


    # PRIMO CICLO: sui turni esistenti

    id_sit=[]

    query_pg='''select id_turno, descrizione, cod_turno,
    inizio_ora, fine_ora, inizio_minuti, fine_minuti
    from elem.turni t '''
    try:
        curr.execute(query_pg)
        turno=curr.fetchall()
    except Exception as e:
        logging.error(query_pg)
        logging.error(e)
    logging.debug(len(turno))
    for t_sit in turno:
        if t_sit[0] in id_uo:
            id_sit.append(int(t_sit[0]))
            k=id_uo.index(t_sit[0])
            if int(t_sit[3])==int(h_s[k]) and int(t_sit[4])==int(h_e[k]) and int(t_sit[5])==int(m_s[k]) and int(t_sit[6])==int(m_e[k]):
                logging.info('ID {} confermato'.format(t_sit[0]))
            else: 
                text='''ID {0} non corrisponde 
                Inizio SIT {1}:{2} Fine SIT: {3}:{4} - 
                Inizio UO {5}:{6} - Fine UO {7}:{8}'''.format(t_sit[0], t_sit[3], t_sit[5], t_sit[4], t_sit[6], int(h_s[k]), m_s[k], h_e[k], m_e[k])
                #text='''ID {} non corrisponde 
                #Inizio SIT {} Fine SIT: {} - '''.format(t_sit[0], t_sit[3], t_sit[4])
                logging.warning(text)
        else:
            # sarebbe da rimuovere
            logging.info('ID {} da rimuovere'.format(t_sit[0]))
            delete= '''DELETE FROM elem.turni
            WHERE id_turno=%s;'''
            curr.execute(delete, (t_sit[0],))
            

    logging.info(' Fine ciclo 1\n ************************************')

    #conn.commit()
    curr.close()
    curr = conn.cursor()
    
    # SECONDO CICLO: su Oracle per aggiungere ciò che non c'è
    i=0
    while i<len(id_uo):
        # update fascia con quelle di SIT
        if fascia[i]=='M':
            f='A'
        else:
            f=fascia[i]
        cod_turno='{} - {}'.format(codice[i], desc[i])
        '''if int(m_s[i])==0 and int(m_e[i])==0:
            cod_turno='{} {}/{}'.format(codice[i], int(h_s[i]), int(h_e[i]))
        else: 
            if int(m_s[i])==0:
               cod_turno='{}  {}/{}:{}'.format(codice[i], int(h_s[i]), int(h_e[i]), m_e[i])
            elif int(m_e[i])==0:
                cod_turno='{}  {}/{}'.format(codice[i], int(h_s[i]), m_s[i], int(h_e[i]))
            else:
                cod_turno='{}  {}:{}/{}:{}'.format(codice[i], int(h_s[i]), m_s[i], int(h_e[i]), m_e[i])
        '''
        logging.debug('I:{} - Descrizione: {} - Codice: {} '.format(id_uo[i], f, cod_turno ))
        
        if id_uo[i] in id_sit:
            update = '''UPDATE elem.turni
                SET descrizione=%s, cod_turno=%s, inizio_ora=%s, fine_ora=%s, inizio_minuti=%s, fine_minuti=%s
                WHERE id_turno=%s;
                '''
            curr.execute(update, (f,cod_turno, h_s[i], h_e[i], m_s[i], m_e[i],id_uo[i],))
        else:    
            insert='''INSERT INTO elem.turni
            (id_turno, descrizione, cod_turno, inizio_ora, fine_ora, inizio_minuti, fine_minuti)
            VALUES(%s, %s, %s, %s, %s, %s, %s);'''
            curr.execute(insert, (id_uo[i],f,cod_turno, h_s[i], h_e[i], m_s[i], m_e[i],))        
        i+=1


    #conn.commit()
    curr.close()
    
    curr = conn.cursor()   
    cur = con.cursor()
    #elenco percorsi attivi
    select_percorsi='''select cod_percorso, id_turno, id_categoria_uso 
        from elem.percorsi where id_categoria_uso in (3,6)'''
    
    
    try:
        curr.execute(select_percorsi)
        percorsi=curr.fetchall()
    except Exception as e:
        logging.error(select_percorsi)
        logging.error(e)
    
    percorsi_anomali_uo=[]
    curr1 = conn.cursor()   
    for pp in percorsi:
        percorso_uo='''SELECT ID_PERCORSO, ID_TURNO 
        FROM ANAGR_SER_PER_UO aspu 
        WHERE ID_PERCORSO IN (:cod_perc) AND DTA_DISATTIVAZIONE > SYSDATE
        GROUP BY ID_PERCORSO, ID_TURNO'''
        try:
            cur.execute(percorso_uo, [pp[0]])
            percorsi_uo=cur.fetchall()
        except Exception as e:
            logging.error(query_pg)
            logging.error(e)
        
        if (len(percorsi_uo))> 1:
            percorsi_anomali_uo.append(pp[0])
            logging.warning('Percorso {} - Anomalia turni'.format(pp[0]))
        
         
        for pp_uo in percorsi_uo:
            if pp[1]!=pp_uo[1]:
                update_turno_sit='''UPDATE elem.percorsi 
                SET id_turno= %s
                WHERE cod_percorso=%s and id_categoria_uso=3'''
                curr1.execute(update_turno_sit, (pp_uo[1],pp[0],))
    
    
    
    curr1.close()
    #conn.commit()
    curr.close()
    cur.close()





    cur = con.cursor()
    if len(percorsi_anomali_uo)>0:
        file_ut="{0}/report/anomalie_turni.xlsx".format(path)
        workbook = xlsxwriter.Workbook(file_ut)
        w1 = workbook.add_worksheet('Anomalie turni')

        w1.set_tab_color('red')

        date_format = workbook.add_format({'num_format': 'dd/mm/yyyy hh:mm'})

        title = workbook.add_format({'bold': True, 'bg_color': '#F9FF33', 'valign': 'vcenter', 'center_across': True,'text_wrap': True})
        text = workbook.add_format({'text_wrap': True, 'bg_color': '#ccffee'})
        date_format = workbook.add_format({'num_format': 'dd/mm/yyyy hh:mm', 'bg_color': '#ccffee'})
        text_dispari= workbook.add_format({'text_wrap': True, 'bg_color': '#ffcc99'})
        date_format_dispari = workbook.add_format({'num_format': 'dd/mm/yyyy hh:mm', 'bg_color': '#ffcc99'})

        w1.set_column(0, 0, 15)
        w1.set_column(1, 2, 30)
        w1.set_column(3, 3, 30)
        w1.set_column(4, 4, 10)
        w1.set_column(5, 5, 40)


        w1.write(0, 0, 'COD_PERCORSO', title) 
        w1.write(0, 1, 'SERVIZIO', title) 
        w1.write(0, 2, 'DATA ATTIVAZIONE', title) 
        w1.write(0, 3, 'UO', title) 
        w1.write(0, 4, 'ID_TURNO', title) 
        w1.write(0, 5, 'DESCRIZIONE_TURNO', title) 
        
        i=0
        r=1
        while i< len(percorsi_anomali_uo):
            select_anomalie= '''SELECT aspu.ID_PERCORSO, as2.DESC_SERVIZIO,
            aspu.DTA_ATTIVAZIONE, au.DESC_UO, aspu.ID_TURNO, at2.DESCR_ORARIO  
            FROM ANAGR_SER_PER_UO aspu 
            JOIN ANAGR_TURNI at2 ON at2.ID_TURNO =aspu.ID_TURNO
            JOIN ANAGR_UO au ON au.ID_UO = aspu.ID_UO 
            JOIN ANAGR_SERVIZI as2 ON as2.ID_SERVIZIO = aspu.ID_SERVIZIO 
            WHERE ID_PERCORSO IN (:id_perc) AND aspu.DTA_DISATTIVAZIONE > SYSDATE'''
            try:
                cur.execute(select_anomalie, [percorsi_anomali_uo[i]])
                anomalie=cur.fetchall()
            except Exception as e:
                logging.error(query_pg)
                logging.error(e)
            for aa in anomalie:
                j=0
                while j<len(aa):
                    if i%2==0:
                        if j==2:
                            w1.write(r, j, aa[j], date_format)
                        else:
                            w1.write(r, j, aa[j], text)
                    else:
                        if j==2:
                            w1.write(r, j, aa[j], date_format_dispari)
                        else:
                            w1.write(r, j, aa[j], text_dispari)
                    j+=1
                r+=1
            i+=1
        
        workbook.close()
        ################################
        # predisposizione mail
        ################################

        # Create a secure SSL context
        context = ssl.create_default_context()

        subject = "WARNING: anomalie turni su UO"
        body = '''Mail generata automaticamente dal codice python turni.py che gira su server amiugis.\n\n
Esistono dei percorsi su UO dove c'è una discordanza di turni fra un UT e l'altra. \nVisualizza l'allegato e contatta le UT di riferimento\n\n\n\n
AMIU Assistenza Territorio
'''
        sender_email = user_mail
        receiver_email='assterritorio@amiu.genova.it'
        debug_email='roberto.marzocchi@amiu.genova.it'
        #cc_mail='pianar@amiu.genova.it'

        # Create a multipart message and set headers
        message = MIMEMultipart()
        message["From"] = sender_email
        message["To"] = receiver_email
        #message["Cc"] = cc_mail
        message["Subject"] = subject
        #message["Bcc"] = debug_email  # Recommended for mass emails
        message.preamble = "Numero di civici alto"

        
                        
        # Add body to email
        message.attach(MIMEText(body, "plain"))

        # aggiunto allegato (usando la funzione importata)
        allegato(message, file_ut, 'anomalie_turni.xlsx')
        
        #text = message.as_string()

        # Now send or store the message
        with smtplib.SMTP_SSL(smtp_mail, port_mail, context=context) as s:
            s.login(user_mail, pwd_mail)
            s.send_message(message) 
        


if __name__ == "__main__":
    main()   