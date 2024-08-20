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





currentdir = os.path.dirname(os.path.realpath(__file__))
parentdir = os.path.dirname(currentdir)
sys.path.append(parentdir)


import logging

#path=os.path.dirname(sys.argv[0]) 

# per scaricare file da EKOVISION




filename = inspect.getframeinfo(inspect.currentframe()).filename
#path = os.path.dirname(os.path.abspath(filename))
path1 = os.path.dirname(os.path.dirname(os.path.abspath(filename)))
path=os.path.dirname(sys.argv[0]) 
path1 = os.path.dirname(os.path.dirname(os.path.abspath(filename)))
nome=os.path.basename(__file__).replace('.py','')
#tmpfolder=tempfile.gettempdir() # get the current temporary directory
logfile='{0}/log/{1}.log'.format(path,nome)
errorfile='{0}/log/error_{1}.log'.format(path,nome)
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



# funzione per dividere un file PDF da pagina X a pagina Y (copiata da https://www.geeksforgeeks.org/working-with-pdf-files-in-python/)



def main():
    
    
    CF_AZIENDA='03818890109'
    file_processati='file_processati.csv'
    
    
    
    
    
    
    
    filenames = []
    
    for filename in os.listdir(path):
        if filename.lower().endswith('.pdf'):
            filenames.append(os.path.join(filename))
            
    #filenames_check = []
    #open and read the file after the appending:
    #f = open(file_processati, "r")
    #print(f.read())     
    
    logger.info('Ho trovato {0} files da processare:{1}'.format(len(filenames), filenames))
    #logger.debug(filenames)
    #logger.debug(filenames_check)
    exit
    k=0
    while k < len(filenames):    
        
        logger.info('Processo il file PDF dal nome {0}, che ho trovato in questa cartella'.format(filenames[k]))
        
        # creating a pdf reader object 
        reader = PdfReader('{0}/{1}'.format(path, filenames[k])) 
        
        # printing number of pages in pdf file 
        logger.info('Il file PDF ha {0} pagine di cui scarto la prima'.format(len(reader.pages)))



        CF=''
        matricola=''
        
        

        i=1
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
                outputpdf='{0}/output/{1}-{2}-{3}-{4}--BLD--{5}.pdf'.format(path,CF_AZIENDA, CF, anno,mese, matricola_old)
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
        f.write('{};{}\n'.format(filenames[k], giorno_file))
        f.close()
        
        k+=1


if __name__ == "__main__":
    main()       