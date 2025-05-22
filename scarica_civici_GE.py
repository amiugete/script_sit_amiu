#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2021
# Roberto Marzocchi

'''
Routine per:
1) lo scarico dei civici di Genova dal geoportale comunale tramite WS WFS,
2) importazione sul DB di SIT
3) calcolo coordinate 
4) aggiorno tabelle geo.civici_rossi e geo.civici_neri
5) update/insert sul DB strade
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
from invio_messaggio import *



#libreria per gestione log
import logging


#num_giorno=datetime.datetime.today().weekday()
#giorno=datetime.datetime.today().strftime('%A')

filename = inspect.getframeinfo(inspect.currentframe()).filename
path     = os.path.dirname(os.path.abspath(filename))


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




debug=0 # da usare per saltare il download in fase di debug su Oracle (1 salta)


def main():




    # carico i mezzi sul DB PostgreSQL
    logger.info('Connessione al db SIT')
    conn = psycopg2.connect(dbname=db,
                        port=port,
                        user=user,
                        password=pwd,
                        host=host)

   
    conn.autocommit = True
    
    # connessione Oracle
    logger.info('Connessione al db STRADE')
    cx_Oracle.init_oracle_client() # necessario configurare il client oracle correttamente
    parametri_con='{}/{}@//{}:{}/{}'.format(user_strade,pwd_strade, host_uo,port_uo,service_uo)
    logger.debug(parametri_con)
    con = cx_Oracle.connect(parametri_con)
    logger.info("Versione ORACLE: {}".format(con.version))
    cur = con.cursor()

    epsg= 4326



    query1 = '''select id, nome_municipio, 
replace(replace(replace(st_extent(st_transform(geom,{}))::text,'BOX(',''),')',''),' ',',')
from geo.municipi_area_comune mac 
group by id, nome_municipio
order by 1'''.format(epsg)

    #print(query1)
    curr3 = conn.cursor()


    try:
        curr3.execute(query1)
        lista_municipi=curr3.fetchall()
    except Exception as e:
        logger.error(e)


    if debug==0: # solo se debug = 0 non faccio download
        for uu in lista_municipi:
            logger.info('Inserimento civici municipio {}'.format(uu[1]))

            endpoint='''https://mappe.comune.genova.it/geoserver/wfs?service=wfs&version=2.0.0'''
            url_ge='''https://mappe.comune.genova.it/geoserver/wfs'''
            layer='''MEDIATORE:V_CIVICI_DBT_ANGOLO_GEOSERVER'''
            
            params={ 'service' : 'wfs',
                        'version': '2.0.0',
                        'request': 'GetFeature',
                        'typeNames' : layer,
                        'outputFormat':'json',
                        'cql_filter': "NOME_MUNICIPIO ILIKE '%s'" %(uu[1])
                        }
                    
            url_2 = urllib.parse.urlencode(params)
            url_dw = '{}?{}'.format(url_ge, url_2)
            
            #url_dw='''{0}&request=GetFeature&typeNames={1}&cql_filter=NOME_MUNICIPIO+ILIKE+'{2}'&outputFormat=json'''.format(endpoint,layer,uu[1])
            #print(url_dw)
            logger.info('m {} di {}'.format(uu[0], len(lista_municipi)))
            nomefile='{0}/civici/{1}.geojson'.format(path,uu[1].replace(' ', '_'))
            #testfile = urllib.URLopener()
            #testfile.retrieve(url_dw, nomefile)
            urllib.request.urlretrieve(url_dw, nomefile)
            curr1 = conn.cursor()
            
            with open(nomefile) as file:
                gj=json.load(file)
                #controllo se il numero dei civici di quel municipio si avvicinasse al numero critico in download
                if len(gj['features'])>49500:
                    ################################
                    # predisposizione mail
                    ################################

                    # Create a secure SSL context
                    context = ssl.create_default_context()

                    subject = "WARNING: numero civici per municipio"
                    body = '''Mail generata automaticamente dal codice python scarica_civici_GE.py che gira su server amiugis\n\n\n:\n
                    Municipio:{}\n
                    Numero civici:{}'''.format(uu[1], len(gj['features']))
                    #sender_email = user_mail
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
                    logger.info("Richiamo la funzione per inviare mail")
                    invio=invio_messaggio(message)
                    logger.info(invio) 
                    
                for feature in gj['features']:
                    ''' "SEZIONE_ELETTORALE":507,
                        "NUMERO":"0024",
                        "LETTERA":"A",
                        "ANGOLO":-88,
                        "TESTO":"24Ar",
                        "ID_OGGETTO_RIFERIMENTO":81184,
                        "DESVIA":"VIA PONTEROTTO",
                        "CODICE_MUNICIPIO":"3",
                        "CODICE_INDIRIZZO":99811,
                        "CODVIA":"50100",
                        "COLORE":"R",
                        "USO":"N",
                        "ID":32450,
                        "SEZIONE_CENSIMENTO_2011":2289,
                        "NOME_MUNICIPIO":"Bassa Val Bisagno",
                        "ID_ASTA":"555544609",
                        "TIPO_OGGETTO_RIFERIMENTO":"IMM_EDIFICIO",
                        "CODICE_CONTROLLO":"501000024A2"
                    '''
                    geom = (json.dumps(feature['geometry']))
                    codvia=feature['properties']['CODVIA']
                    numero=feature['properties']['NUMERO']
                    lettera=feature['properties']['LETTERA']
                    colore=feature['properties']['COLORE']
                    testo=feature['properties']['TESTO']
                    uso=feature['properties']['USO']
                    id_municipio=feature['properties']['CODICE_MUNICIPIO']
                    sez_censimento=feature['properties']['SEZIONE_CENSIMENTO_2011']
                    codice_indirizzo_comune=feature['properties']['CODICE_INDIRIZZO']
                    # calcolo il codice civico AMIU
                    if lettera==None:
                        l='_'
                    else:
                        l=lettera
                    if colore=='R':
                        c=colore
                    else:
                        c='_'
                    cod_civico='{0}{1}{2}{3}'.format(codvia,numero,l,c)
                    select="SELECT cod_civico from etl.civici_comune where cod_civico = %s"
                    try:
                        curr1.execute(select,(cod_civico,))
                        lista_civici=curr1.fetchall()
                    except Exception as e:
                        logger.error(e)
                    if len(lista_civici)==1:
                        #update
                        update='''UPDATE etl.civici_comune set 
                        cod_strada=%s,
                        numero=%s,
                        lettera=%s,
                        colore=%s,
                        testo=%s,
                        uso=%s,
                        id_municipio=%s,
                        codice_indirizzo_comune=%s,
                        sez_censimento=%s, 
                        geoloc=ST_SetSRID(ST_GeomFromGeoJSON(%s),3003),
                        mod_date=now()
                        where cod_civico=%s
                        '''
                        curr1.execute(update,(codvia, numero, lettera,colore, testo,uso, id_municipio, codice_indirizzo_comune, sez_censimento, geom, cod_civico,))
                    else:
                        #insert
                        try:
                            insert='''insert into etl.civici_comune (cod_strada, numero, lettera,colore, testo,
                        uso, id_municipio, codice_indirizzo_comune, geoloc, cod_civico, sez_censimento) VALUES ( %s,%s,%s,%s,%s,
                        %s,%s,%s,ST_SetSRID(ST_GeomFromGeoJSON(%s),3003),%s, %s)'''
                            curr1.execute(insert,(codvia, numero, lettera,colore, testo,uso, id_municipio, codice_indirizzo_comune, geom, cod_civico,sez_censimento,))
                        except Exception as e:
                            logger.error(e)
                            logger.error('cod_civico={}, geom={}'.format(cod_civico,geom))
                    
                            
                            
                #conn.commit()
                

            curr1.close()
            
        
    
    # aggiorno tabelle geo
    
    
    curr = conn.cursor()
    query_upsert_neri='''INSERT INTO geo.civici_neri (geoloc, cod_strada, numero, lettera, colore, testo, cod_civico, ins_date, mod_date)
select geoloc, 
cod_strada::text, 
numero::int::text, 
lettera, 
1 as colore, 
testo, cod_civico, now() as ins_date, 
null as mod_date
from etl.civici_comune cc
where colore is null  
ON CONFLICT (cod_civico) 
DO UPDATE  SET geoloc=EXCLUDED.geoloc,
cod_strada=EXCLUDED.cod_strada, 
numero=EXCLUDED.numero, 
lettera=EXCLUDED.lettera, 
colore=1, 
testo=EXCLUDED.testo, 
cod_civico=EXCLUDED.cod_civico, 
ins_date=EXCLUDED.ins_date, 
mod_date=now()'''
    try:
        curr.execute(query_upsert_neri)
    except Exception as e:
        logger.error(query_upsert_neri)
        logger.error(e)
    curr.close()
    curr = conn.cursor()
    query_upsert_neri='''INSERT INTO geo.civici_rossi (geoloc, cod_strada, numero, lettera, colore, testo, cod_civico, ins_date, mod_date)
select geoloc, 
cod_strada::text, 
numero::int::text, 
lettera, 
1 as colore, 
testo, cod_civico, now() as ins_date, 
null as mod_date
from etl.civici_comune cc
where colore = 'R'  
ON CONFLICT (cod_civico) 
DO UPDATE  SET geoloc=EXCLUDED.geoloc,
cod_strada=EXCLUDED.cod_strada, 
numero=EXCLUDED.numero, 
lettera=EXCLUDED.lettera, 
colore=2, 
testo=EXCLUDED.testo, 
cod_civico=EXCLUDED.cod_civico, 
ins_date=EXCLUDED.ins_date, 
mod_date=now()'''
    curr.close()
    
    
    
    
    # aggiorno strade    
    civici_GE= '''select cod_civico,
cod_strada as id_via,
numero as numero_civico, 
lettera as lettera_civico,
colore as colore_civico, 
testo as descrizione_civico,
st_y(st_transform(geoloc,4326)) as coord_lat, 
st_x(st_transform(geoloc,4326)) as coord_lon,
sez_censimento
from etl.civici_comune cc'''
    #curr = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    curr = conn.cursor()
    try:
        curr.execute(civici_GE)
        lista_civici=curr.fetchall()
    except Exception as e:
        logger.error(e)
    logger.info ('Copio i dati su Oracle')
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
        (COD_CIVICO, ID_VIA, NUMERO_CIVICO, LETTERA_CIVICO, COLORE_CIVICO, DESCRIZIONE_CIVICO, COORD_LAT, COORD_LON, SEZIONE_CENSIMENTO_2011)
        VALUES(:c_c, :iv, :nc, :lc, :col, :tc, :lat, :lon, :sez)'''
        #cur.execute(insert_o, data)
        cur.execute(insert_o, [cc[0], cc[1], cc[2], lc_temp, col_temp, cc[5], cc[6], cc[7], cc[8]])
    con.commit() 
    #cur.commit() 
    logger.info("Fine copia dati su DB Oracle")      
        
         
            
                
        
       
        
    curr.close()
    conn.close()
    
    cur.close()
    con.close()

    # check se c_handller contiene almeno una riga 
    error_log_mail(errorfile, 'roberto.marzocchi@amiu.genova.it', os.path.basename(__file__), logger)



if __name__ == "__main__":
    main()   