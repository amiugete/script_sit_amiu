#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2021
# Roberto Marzocchi

'''
Routine per:
1) lo scarico dei civici di Genova dal geoportale comunale tramite WS WFS,
2) importazione sul DB di SIT
3) calcolo coordinate 
4) update/insert sul DB strade
'''


from doctest import ELLIPSIS_MARKER
import os,sys, getopt
import inspect, os.path


import urllib.request

import json

import psycopg2
import psycopg2.extras

import cx_Oracle

import datetime

from urllib.request import urlopen
import urllib.parse

currentdir = os.path.dirname(os.path.realpath(__file__))
parentdir = os.path.dirname(currentdir)
sys.path.append(parentdir)

from credenziali import *
#from credenziali import db, port, user, pwd, host, user_mail, pwd_mail, port_mail, smtp_mail



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



#libreria per gestione log
import logging


#num_giorno=datetime.datetime.today().weekday()
#giorno=datetime.datetime.today().strftime('%A')

filename = inspect.getframeinfo(inspect.currentframe()).filename
path     = os.path.dirname(os.path.abspath(filename))


giorno_file=datetime.datetime.today().strftime('%Y%m%d')


logfile='{}/log/{}_civici_fuorige.log'.format(path, giorno_file)

logging.basicConfig(
    handlers=[logging.FileHandler(filename=logfile, encoding='utf-8', mode='a')],
    format='%(asctime)s\t%(levelname)s\t%(message)s',
    #filemode='w', # overwrite or append
    #fileencoding='utf-8',
    #filename=logfile,
    level=logging.INFO)

debug=0 # da usare per saltare il download in fase di debug su Oracle (1 salta)


def main():




    # carico i mezzi sul DB PostgreSQL
    logging.info('Connessione al db SIT')
    conn = psycopg2.connect(dbname=db,
                        port=port,
                        user=user,
                        password=pwd,
                        host=host)

   
    conn.autocommit = True
    
    # connessione Oracle
    logging.info('Connessione al db STRADE')
    cx_Oracle.init_oracle_client() # necessario configurare il client oracle correttamente
    parametri_con='{}/{}@//{}:{}/{}'.format(user_strade,pwd_strade, host_uo,port_uo,service_uo)
    logging.debug(parametri_con)
    con = cx_Oracle.connect(parametri_con)
    logging.info("Versione ORACLE: {}".format(con.version))
    cur = con.cursor()

    epsg= 4326



    query1 = '''select cw.id_comune, c.descr_comune,
    cw.wfs_civici_ai, cw.wfs_civici_ac 
    from marzocchir.comuni_wfs cw 
join topo.comuni c on c.id_comune = cw.id_comune '''

    #print(query1)
    curr3 = conn.cursor()


    try:
        curr3.execute(query1)
        lista_municipi=curr3.fetchall()
    except Exception as e:
        logging.error(e)

    tr="TRUNCATE geo.civici_fuori_ge"
    try:
        curr3.execute(tr)
    except Exception as e:
        logging.error(e)
    curr1 = conn.cursor()
    layers=['civici', 'carrai']
    indice=[2,3] # indice in base alla precedente select 
    if debug==0: # solo se debug = 0 non faccio download
        cont=0
        for uu in lista_municipi:
            k=0
            cont+=1
            while k<len(layers):
                logging.info('Inserimento civici comune di {} ({})'.format(uu[1], layers[k]))

                endpoint='''https://mappe.comune.genova.it/geoserver/wfs?service=wfs&version=2.0.0'''
                url='''https://geoservizi.regione.liguria.it/geoserver/wfs?'''
                layer=uu[indice[k]]
                
                params={ 'service' : 'wfs',
                            'version': '2.0.0',
                            'request': 'GetFeature',
                            'typeNames' : layer,
                            'outputFormat':'json',
                            }
                        
                url_2 = urllib.parse.urlencode(params)
                url_dw = '{}?{}'.format(url, url_2)
                
                #url_dw='''{0}&request=GetFeature&typeNames={1}&cql_filter=NOME_MUNICIPIO+ILIKE+'{2}'&outputFormat=json'''.format(endpoint,layer,uu[1])
                print(url_dw)
                
                print('m {} ({}/2) di {}'.format(cont,k+1, len(lista_municipi)))
                nomefile='{0}/civici/{1}_{2}.geojson'.format(path,uu[1].replace(' ', '_'), layers[k])
                #testfile = urllib.URLopener()
                #testfile.retrieve(url_dw, nomefile)
                try:
                    urllib.request.urlretrieve(url_dw, nomefile)
                    
                    
                    with open(nomefile) as file:
                        gj=json.load(file)
                        print(len(gj['features']))
                        #controllo se il numero dei civici di quel municipio si avvicinasse al numero critico in download
                        if len(gj['features'])>49500 or len(gj['features'])==0:
                            ################################
                            # predisposizione mail
                            ################################

                            # Create a secure SSL context
                            context = ssl.create_default_context()

                            subject = "WARNING: numero civici per municipio"
                            body = '''Mail generata automaticamente dal codice python scarica_civici_GE.py che gira su server amiugis\n\n\n:\n
                            Municipio:{}\n
                            Numero civici:{}'''.format(uu[1], len(gj['features']))
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

                            #text = message.as_string()

                            # Now send or store the message
                            with smtplib.SMTP_SSL(smtp_mail, port_mail, context=context) as s:
                                s.login(user_mail, pwd_mail)
                                s.send_message(message) 
                            
                        for feature in gj['features']:
                            geom = (json.dumps(feature['geometry']))
                            idvia=feature['properties']['civ_id_via']
                            numero=feature['properties']['civ_numero']
                            cod_istat=feature['properties']['eca_codice_istat']
                            civ_lettera=feature['properties']['civ_esponente']
                            eca_id=feature['properties']['eca_id']
                            eca_tipo=feature['properties']['eca_tipo']
                            id_civ_reg=feature['properties']['civ_id']
                            eca_id_tipo_via=feature['properties']['eca_id_tipo_via']
                            tipo_via=feature['properties']['tipo_via']
                            #colore=feature['properties']['COLORE']
                            testo=feature['properties']['civico']
                            note=feature['properties']['civ_note']
                            provvisorio=feature['properties']['civ_provvisorio']
                            nome_via_ufficiale=feature['properties']['via_nome_ufficiale']
                            #print(k)
                            #print(layers[k])
                            if layers[k]=='carrai':
                                c='true'
                            else:
                                c='false'
                            if testo is None:
                                lettera = None
                            else:
                                tt=testo.split('\\')
                                if len(tt)==1:
                                    lettera=None
                                else:
                                    lettera=tt[1]
                            # calcolo il codice civico AMIU
                            '''if lettera==None:
                                l='_'
                            else:
                                l=lettera
                            if colore=='R':
                                c=colore
                            else:
                                c='_'
                            cod_civico='{0}{1}{2}{3}'.format(codvia,numero,l,c)
                            '''
                            
                        
                            try:
                                insert='''insert into geo.civici_fuori_ge (id_via_comune, numero, lettera,testo,
                            note, id_comune, geoloc, provvisorio, nome_via_ufficiale, carraio, comune_istat,
                            eca_id, eca_tipo, id_civico_regione,eca_id_tipo_via,tipo_via)  VALUES ( %s,%s,%s,%s,
                            %s,%s,ST_SetSRID(ST_Force2D(ST_GeomFromGeoJSON(%s)),3003),%s, %s,%s, %s,
                            %s,%s,%s,%s,%s)'''
                                curr1.execute(insert,(idvia, numero, civ_lettera, testo, note, uu[0], geom, provvisorio,nome_via_ufficiale,c,cod_istat,eca_id, eca_tipo, id_civ_reg,eca_id_tipo_via,tipo_via))
                            except Exception as e:
                                logging.error(e)
                                logging.error('nome_via={}, civ={}, geom={}'.format(nome_via_ufficiale, testo, geom))
                except Exception as e:
                    logging.error(e)
                    logging.error('Problema download civici comune di {} ({})'.format(uu[1], layers[k]))
                    
                
                k+=1
                            
                #conn.commit()
                

    curr1.close()
    exit()
    civici_GE= '''select cod_civico,
cod_strada as id_via,
numero as numero_civico, 
lettera as lettera_civico,
colore as colore_civico, 
testo as descrizione_civico,
st_y(st_transform(geoloc,4326)) as coord_lat, 
st_x(st_transform(geoloc,4326)) as coord_lon
from geo.civici_comune cc'''
    #curr = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    curr = conn.cursor()
    try:
        curr.execute(civici_GE)
        lista_civici=curr.fetchall()
    except Exception as e:
        logging.error(e)
    logging.info ('Copio i dati su Oracle')
    cur.execute('TRUNCATE TABLE CIVICI_DA_COMUNE')
    con.commit()
    for cc in lista_civici:
        #lista_ok=['' if v is None else v for v in cc]
        #print(lista_ok)
        #res = [str(i or '') for i in cc]
        #print(cc)
        #print(res)
        #cur.setinputsizes(11, int, 4, 1, 1, 25, float, float)
        if cc[3]== None:
            lc_temp=""
        else:
            lc_temp=cc[3] 
        if cc[4]== None:
            col_temp=""
        else:
            col_temp=cc[4] 
        #dict = {"cc": cc[0], "iv": cc[1], "nc": cc[2], "lc": lc, "col": col, "tc":cc[5], "lat":cc[6], "lon":cc[7]}
        data = dict(c_c=cc[0],  iv= cc[1], nc= cc[2], lc= lc_temp, col= col_temp, tc=cc[5], lat=cc[6], lon=cc[7])
        #print(data)
        cur.setinputsizes(11, int, 4, 1, 1, 25, float, float) 
        insert_o='''INSERT INTO STRADE.CIVICI_DA_COMUNE
        (COD_CIVICO, ID_VIA, NUMERO_CIVICO, LETTERA_CIVICO, COLORE_CIVICO, DESCRIZIONE_CIVICO, COORD_LAT, COORD_LON)
        VALUES(:c_c, :iv, :nc, :lc, :col, :tc, :lat, :lon)'''
        #cur.execute(insert_o, data)
        cur.execute(insert_o, [cc[0], cc[1], cc[2], lc_temp, col_temp, cc[5], cc[6], cc[7]])
    con.commit() 
    #cur.commit() 
    logging.info("Fine copia dati su DB Oracle")      
        
         
            
                
        
       
        
    curr.close()





if __name__ == "__main__":
    main()   