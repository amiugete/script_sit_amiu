#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2024
# Roberto Piccardo, Roberto Marzocchi

'''
1) Apro file PDF

2) Leggo e verifico il codice fiscale

3) Splitto il file a parità di codice fiscale 


'''

#from msilib import type_short
import os, sys, re  # ,shutil,glob

import inspect, os.path
#import getopt  # per gestire gli input

#import pymssql

from datetime import date, datetime, timedelta



# libreria PDF
from pypdf import PdfReader, PdfWriter 



import csv

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



# inizializzo i nomi dei file di log (per capire cosa stia succedendo)
logfile='{0}/log/{1}.log'.format(path,nome)
errorfile='{0}/log/warning_error_{1}.log'.format(path,nome)







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


c_handler.setLevel(logging.WARNING)
f_handler.setLevel(logging.DEBUG)


# Add handlers to the logger
logger.addHandler(c_handler)
logger.addHandler(f_handler)


cc_format = logging.Formatter('%(asctime)s\t%(levelname)s\t%(message)s')

c_handler.setFormatter(cc_format)
f_handler.setFormatter(cc_format)







def main():
    
    # PARAMETRI INIZIALI 
    CFS_AZIENDE=['03818890109', '01266290996', '01426960991']
    AZIENDE=['AMIU', 'BONIFICHE', 'SATER']
    file_processati='file_processati.csv'
    
    # anomalie
    a_anno=[]
    a_CF=[]
    a_file=[]
    file_anomalie='file_anomalie.csv'
    
    intestazione='''CERTIFICAZIONE DI CUI ALL'ART'''
    
    mesi_italiano=['GENNAIO', 
                   'FEBBRAIO',
                   'MARZO',
                   'APRILE',
                   'MAGGIO',
                   'GIUGNO',
                   'LUGLIO',
                   'AGOSTO',
                   'SETTEMBRE',
                   'OTTOBRE',
                   'NOVEMBRE',
                   'DICEMBRE']
    
    filenames_check = []
    
    with open('{0}/{1}'.format(path,file_processati), mode ='r') as file:
        csvFile = csv.reader(file,  delimiter=';')
        for ll in csvFile:
            filenames_check.append(ll[0])
    
    #logger.debug(filenames_check)
    #exit()
    
    
    filenames = []
    cf_aziende_file=[]
    folder_aziende=[]
    
    a=0
    while a<len(AZIENDE):
        for filename in os.listdir('{0}/input/cu/{1}'.format(path, AZIENDE[a])):
            if filename.lower().endswith('.pdf')and filename not in filenames_check:
                filenames.append(os.path.join(filename))
                cf_aziende_file.append(CFS_AZIENDE[a])
                folder_aziende.append(AZIENDE[a])
        a+=1
        
    
    logger.debug(filenames)
    folder_aziende    
            
    #filenames_check = []
    #open and read the file after the appending:
    #f = open(file_processati, "r")
    #print(f.read())     
    
    logger.info('Ho trovato {0} files da processare:{1}'.format(len(filenames), filenames))
    
    
    if len(filenames)==0:
        logger.warning('Non ci sono file da processare. Controlla le cartelle di input e/o il file CSV con i file processati')
    #logger.debug(filenames)
    #logger.debug(filenames_check)

    k=0
    while k < len(filenames):    
        
        logger.info('Processo il file PDF dal nome {0}, che ho trovato in questa cartella'.format(filenames[k]))
        
        # creating a pdf reader object 
        reader = PdfReader('{0}/input/cu/{2}/{1}'.format(path, filenames[k], folder_aziende[k])) 
        
        # printing number of pages in pdf file 
        logger.info('Il file PDF ha {0} pagine'.format(len(reader.pages)))



        CF=''
        matricola=''
        
        

        i=0 # impostando 1 salto la prima pagina, se non volessi saltarla dovrei mettere 0 
        count_doc=0
        while i<len(reader.pages):
            # creating a page object 
            page = reader.pages[i] 
        
            text=page.extract_text() 
            # Split the text into lines 
            lines = text.splitlines() 
            
            # solo per il debug cerco di capire a quali righe leggo le informazioni corrette
            
            '''logger.debug(len(lines)) 
            j=0
            while j<len(lines):
                logger.debug('{}, {}'.format(j,lines[j]))
                j+=1         
            '''
            
            
            #exit()
            # controllo se c'è l'intestazione 
            cc=0
            check_intestazione =0
            while cc <len(lines):
                if intestazione in lines[cc]:
                    check_intestazione=1
                cc+=1


            if len(lines)>33 and check_intestazione==1: # (intestazione in lines[0] or intestazione in lines[34]  or intesazione in lines):
                logger.debug('sono nella prima pagina di una CU')   
                
                # per il debug
                j=0
                while j<len(lines):
                    logger.debug('{}, {}'.format(j,lines[j]))
                    j+=1         
                
                ultima_riga_divisa=lines[len(lines)-1].split('-')
                try:
                    matricola= int(ultima_riga_divisa[len(ultima_riga_divisa)-1])
                except:
                    matricola = 0
                
                
                # questo non funziona se la CU arriva a uno che non è dipendente... caso da analizzare
                '''
                if  len(ultima_riga_divisa)>1:
                    # cognome=ultima_riga_divisa[1]
                    # nome=ultima_riga_divisa[2]
                    check_cf=0
                    j=0
                    while k<len(lines) and check_cf==0:
                        if ultima_riga_divisa[1] in lines[j] and ultima_riga_divisa[2] in lines[j]:
                            # dovrei trovare all'inizio il CF
                            CF=lines[j].split()[0].strip()
                            check_cf=1
                        j+=1
                    # la riga dovrebbe essere sempre quella   
                else: # provo con la riga 66 
                     CF=lines[66].split()[0].strip()
                     
                '''
                #logger.debug(int(lines[len(lines)-2].split()[2].strip())-1)
                
                try:
                    anno = int(lines[len(lines)-2].split()[2].strip())-1
                except:
                    try: 
                        anno = int(lines[len(lines)-1].split()[2].strip())-1
                    except Exception as e:
                        logger.error(e)
                        logger.error('''Non trovo l'anno''')
                
                
                '''
                try: 
                    anno=int(lines[len(lines)-10].strip())
                except:
                    try: 
                        anno=int(lines[len(lines)-11].strip())
                    except:
                        anno=int(lines[len(lines)-2].split()[2].strip())-1
                '''
                check_cf=0
                # lo leggo 4 righe sopra l'ultima riga
                CF=lines[len(lines)-5].split()[0].strip()
                # se è un dipendente faccio controllo che CF sia quello del dipendente.. (non vale per eredi)
                if  len(ultima_riga_divisa)>1:
                    if ultima_riga_divisa[1] in lines[len(lines)-5] and ultima_riga_divisa[2] in lines[len(lines)-5]:
                        check_cf=1
                        #ok
                        
                    if check_cf==0:
                        logger.warning('CF {} non appartiene al dipendente con matricola {} di nome {} {}'.format(CF, matricola, ultima_riga_divisa[1], ultima_riga_divisa[2]))
                    
                logger.debug('Matricola = {}'.format(matricola)) 
                logger.debug('CF = {}'.format(CF)) 
                logger.debug(anno)
                
                
            
            

            
                #inizializzo la scrittura del file
                writer = PdfWriter()
                #creo nuovo file
                path_cu='{0}/output/cu'.format(path)
                path_anno='{0}/{1}'.format(path_cu, anno)
                if not os.path.exists(path_anno):
                    os.makedirs(path_anno)
                outputpdf='{0}/{1}-{2}-{3}-12--CUD--{4}.pdf'.format(path_anno,cf_aziende_file[k], CF, anno, matricola)
                if os.path.isfile(outputpdf):
                    outputpdf='{0}/{1}-{2}-{3}--BLD--{4}_bis.pdf'.format(path_anno, cf_aziende_file[k], CF, anno, matricola)
                    a_anno.append(anno)
                    a_CF.append(CF)
                    a_file.append(outputpdf)
                count_doc+=1
            else:
                # non creo nuovo file
                logger.debug('sono alla pagina {0}. Più pagine per stesso dipendente CF: {1}, Matr:{2}'.format(i, CF, matricola))
                
            # aggiungo la pagina al file
            writer.add_page(reader.pages[i]) 
            # esporto il file 
            with open(outputpdf, "ab") as f: 
                writer.write(f)
                
            i+=1
            #exit()
        
    
        
        

        giorno_file=datetime.today().strftime('%Y/%m/%d %H:%M:%S')
        f = open('{}/{}'.format(path, file_processati), "a")
        f.write('{};{};{}\n'.format(filenames[k], giorno_file,count_doc))
        f.close()
        
        k+=1


    aa=0
    f2 = open('{}/{}'.format(path, file_anomalie), "a")
    while aa<len(a_file):
        f2.write('cu;{0};;{1};{2}\n'.format(a_anno[aa], a_CF[aa], a_file[aa]))
        aa+=1
    f2.close()
    
if __name__ == "__main__":
    main()       