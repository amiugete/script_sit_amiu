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
errorfile='{0}/log/error_{1}.log'.format(path,nome)







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







def main():
    
    # PARAMETRI INIZIALI 
    CFS_AZIENDE=['03818890109', '01266290996', '01426960991']
    AZIENDE=['AMIU', 'BONIFICHE', 'SATER']
    file_processati='file_processati.csv'
    
    # anomalie
    a_anno=[]
    a_mese=[]
    a_CF=[]
    a_file=[]
    file_anomalie='file_anomalie.csv'
    
    
    filenames_check = []
    
    with open('{0}/{1}'.format(path,file_processati), mode ='r') as file:
        csvFile = csv.reader(file,  delimiter=';')
        for ll in csvFile:
            filenames_check.append(ll[0])
    
    logger.debug(filenames_check)
    #exit()
    
    
    
    filenames = []
    cf_aziende_file=[]
    folder_aziende=[]
    
    a=0
    while a<len(AZIENDE):
        for filename in os.listdir('{0}/input/cartellini/{1}'.format(path, AZIENDE[a])):
            if filename.lower().endswith('.pdf')and filename not in filenames_check:
                filenames.append(os.path.join(filename))
                cf_aziende_file.append(CFS_AZIENDE[a])
                folder_aziende.append(AZIENDE[a])
        a+=1
        

    #filenames_check = []
    #open and read the file after the appending:
    #f = open(file_processati, "r")
    #print(f.read())     
    
    logger.info('Ho trovato {0} files da processare:{1}'.format(len(filenames), filenames))
    #logger.debug(filenames)
    #logger.debug(filenames_check)

    if len(filenames)==0:
        logger.warning('Non ci sono file da processare. Controlla le cartelle di input e/o il file CSV con i file processati') 


    k=0
    while k < len(filenames):    
        
        logger.info('Processo il file PDF dal nome {0}, che ho trovato in questa cartella'.format(filenames[k]))
        
        # creating a pdf reader object 
        reader = PdfReader('{0}/input/cartellini/{2}/{1}'.format(path, filenames[k],folder_aziende[k])) 
        
        # printing number of pages in pdf file 
        logger.info('Il file PDF ha {0} pagine di cui scarto la prima'.format(len(reader.pages)))



        CF=''
        matricola=''
        
        

        i=0 # impostando 1 salto la prima pagina, se non volessi saltarla dovrei mettere 0 
        count_doc=1
        while i<len(reader.pages):
            # creating a page object 
            page = reader.pages[i] 
        
            text=page.extract_text() 
            # Split the text into lines 
            lines = text.splitlines() 
            
            #logger.debug(len(lines)) 
            # Iterate through each line 
            
            if len(lines)> 4:
                presenze=lines[2]
                persona=lines[3]
                mese_anno=presenze.split('PRESENZE DEL MESE')[1].strip().split('SEDE')[0]
                anno=int(mese_anno.split('/')[1])
                mese=int(mese_anno.split('/')[0])
                
                #logger.debug(mese)
                matricola_old=matricola
                CF_old=CF
                
                matricola= int(persona.split()[1].strip())
                #logger.debug(matricola)
                CF= persona.split()[len(persona.split())-1].strip()
                #logger.debug(CF)
            #exit()
            
            

            if CF!=CF_old:
                #inizializzo la scrittura del file
                writer = PdfWriter()
                #creo nuovo file
                path_cartellini='{0}/output/cartellini'.format(path)
                path_anno='{0}/{1}'.format(path_cartellini, anno)
                if not os.path.exists(path_anno):
                    os.makedirs(path_anno)
                path_mese='{0}/{1}'.format(path_anno, mese)
                if not os.path.exists(path_mese):
                    os.makedirs(path_mese)
                outputpdf='{0}/{1}-{2}-{3}-{4}--BLD--{5}.pdf'.format(path_mese, cf_aziende_file[k], CF, anno,mese, matricola)
                if os.path.isfile(outputpdf):
                        outputpdf='{0}/{1}-{2}-{3}-{4}--BLD--{5}_bis.pdf'.format(path_mese, cf_aziende_file[k], CF, anno,mese, matricola)
                        a_anno.append(anno)
                        a_mese.append(mese)
                        a_CF.append(CF)
                        a_file.append(outputpdf)
                count_doc+=1
            else:
                # non creo nuovo file
                logger.warning('sono alla pagina {0}. Due pagine per stesso dipendente CF: {1}, Matr:{2}'.format(i, CF, matricola))
                logger.warning ('il file {0} è costituito da 2 pagine'.format(outputpdf))
                
            # aggiungo la pagina al file
            writer.add_page(reader.pages[i]) 
            # esporto il file 
            with open(outputpdf, "ab") as f: 
                writer.write(f)
                
            i+=1
            #exit()
        
    
        
        

        giorno_file=datetime.today().strftime('%Y/%m/%d %H:%M:%S')
        f = open('{}/{}'.format(path, file_processati), "a")
        f.write('{};{};{}\n'.format(filenames[k], giorno_file, count_doc))
        f.close()
        
        k+=1


if __name__ == "__main__":
    main()       