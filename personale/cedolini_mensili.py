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
    # 0 AMIU
    # 1 Bonifiche
    CFS_AZIENDE=['03818890109', '01266290996', '01426960991']
    
    file_processati='file_processati.csv'
    
    # anomalie
    a_anno=[]
    a_mese=[]
    a_CF=[]
    a_file=[]
    file_anomalie='file_anomalie.csv'
    
    
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
                   'DICEMBRE', 
                   'TREDIC']
    
    
    
    filenames_check = []
    
    with open('{0}/{1}'.format(path,file_processati), mode ='r') as file:
        csvFile = csv.reader(file,  delimiter=';')
        for ll in csvFile:
            filenames_check.append(ll[0])
    
    #logger.debug(filenames_check)
    #exit()
    
    
    filenames = []
    
    for filename in os.listdir('{0}/input/cedolini'.format(path)):
        if filename.lower().endswith('.pdf') and filename not in filenames_check:
            filenames.append(os.path.join(filename))
            
    
    logger.info('Ho trovato {0} files da processare:{1}'.format(len(filenames), filenames))
    #logger.debug(filenames)
    #logger.debug(filenames_check)
    
    if len(filenames)==0:
        logger.warning('Non ci sono file da processare. Controlla le cartelle di input e/o il file CSV con i file processati')
    #exit()
    k=0
    while k < len(filenames):    
        
        logger.info('Processo il file PDF dal nome {0}, che ho trovato in questa cartella'.format(filenames[k]))
        
        # creating a pdf reader object 
        reader = PdfReader('{0}/input/cedolini/{1}'.format(path, filenames[k])) 
        
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
            #'''
            logger.debug(len(lines)) 
            kk=0
            check_azienda=0
            while kk<len(lines):
                logger.debug('{}, {}'.format(kk,lines[kk]))
                if check_azienda==0 and CFS_AZIENDE[0] in lines[kk]:
                    CF_AZIENDA= CFS_AZIENDE[0]
                    check_azienda=1
                    logger.warning('AMIU')
                elif check_azienda==0 and CFS_AZIENDE[1] in lines[kk]:
                    CF_AZIENDA= CFS_AZIENDE[1]   
                    logger.warning('Bonifiche')
                    check_azienda=1
                elif check_azienda==0 and CFS_AZIENDE[2] in lines[kk]:
                    CF_AZIENDA= CFS_AZIENDE[2]   
                    logger.warning('S.A.TER. SPA')
                    check_azienda=1
                kk+=1  
                
            
            if check_azienda==0:
                logger.warning(i)
                logger.warning('Non trovo la partita IVA nè di AMIU Bonifiche nè di AMIU. Controllare lo script')
                #exit()

            else:        

                logger.debug(i)
                if len(lines)> 54:
                    
                    #logger.debug(mese)
                    matricola_old=matricola
                    CF_old=CF
                    
                    # cercp il CF aziendale
                    
                    
                    matricola=lines[0].split()[1].replace(',', '')
                    nome=lines[0].split()[2]
                    
                    n=3
                    while n < len(lines[0].split()):
                        nome='{0} {1}'.format(nome,lines[0].split()[n])
                        n+=1
                    
                    
                    '''
                    per cercare il CF posso avere 2 casi 
                    
                    caso 1) lo trovo nella riga 54 (se non c'è indirizzo AMIU nell'intestazione)
                    caso 2) lo trovo nella riga 55 (se c'è indirizzo AMIU nell'intestazione)
                    caso 3) Bonifiche linea 53
                    
                    '''
                    
                
                    
                    
                    CF=lines[54].replace(nome.upper(),'')[:16]
                    if len(CF.strip())<16: #in questo caso dovrei essere nella caso 2
                        #logger.debug('''c'è indirizzo amiu nell'intestazione e il codice fiscale è alla riga 55''')
                        CF=lines[55].replace(nome.upper(),'')[:16]
                    
                    # cerco se c'è uno slash e nel caso vado alla linea 53 
                    if '/' in CF:
                        CF=lines[53].replace(nome.upper(),'')[:16]
                    
                    
                    
                    '''
                    per cercare il periodo posso avere 2 casi 
                    
                    caso 1) lo trovo nella riga - 3 con il conto corrente
                    caso 2) dove il cedolino è su 2 pagine nella prima pagina delle 2 lo trovo nella riga -2
                    
                    
                    '''
                    logger.debug(lines[(len(lines)-3)].strip())
                    logger.debug(lines[(len(lines)-2)].strip())
                    #logger.debug(len(lines[(len(lines)-3)].strip()))
                    
                    check_periodo=0

                    # CASO 1 (vedi sopra)
                    m=0
                    while m<len(mesi_italiano):
                        #logger.debug(m)
                        # se manca IBAN mese in posizione 0, se no in posizione 1 assieme a IBAN
                        if len(lines[(len(lines)-3)].strip().split())>1:
                            if mesi_italiano[m] in lines[(len(lines)-3)].strip().split()[0] or mesi_italiano[m] in lines[(len(lines)-3)].strip().split()[1] :
                                logger.debug('sono nel caso 1')
                                mese=str(m+1).rjust(2,'0')
                                check_periodo=1
                        m+=1
                    
                    
                    # CASO 2    
                    if check_periodo==0:
                        logger.debug('sono nel caso 2')
                        m=0
                        while m<len(mesi_italiano):
                            #logger.debug(m)
                            if len(lines[(len(lines)-2)].strip().split())>1:
                                #logger.debug(mesi_italiano[m])
                                if mesi_italiano[m] in lines[(len(lines)-2)].strip().split()[0] or mesi_italiano[m] in lines[(len(lines)-2)].strip().split()[1]:
                                    mese=str(m+1).rjust(2,'0')
                                    check_periodo=1
                            else:
                                if mesi_italiano[m] in lines[(len(lines)-2)].strip().split()[0] :
                                    mese=str(m+1).rjust(2,'0')
                                    check_periodo=1
                            m+=1
                            # ANNO CASO 2
                            try:
                                anno = int(lines[(len(lines)-2)].strip().split()[2])
                                if anno < 2020:
                                    anno = int(lines[(len(lines)-2)].strip().split()[1][:4])
                            except:
                                anno = int(lines[(len(lines)-2)].strip().split()[1][:4])
                    # ANNO CASO 1
                    else:      
                        # se manca IBAN sono nella posizione 1 e non 2 
                        try:
                            anno = int(lines[(len(lines)-3)].strip().split()[2])
                            if anno < 2020:
                                anno = int(lines[(len(lines)-3)].strip().split()[1][:4])
                        except:
                            anno = int(lines[(len(lines)-3)].strip().split()[1][:4])
                    
                    if anno < 2020:
                        logger.error('ANNO {} SBAGLIATO'.format(anno)) 
                        exit()
                    
                    logger.debug(matricola)
                    logger.debug(nome)
                    logger.debug(CF)
                    logger.debug(mese)
                    logger.debug(anno)
                    
                    
                
                    
                #exit()
                
                

                if CF!=CF_old:
                    #inizializzo la scrittura del file
                    writer = PdfWriter()
                    #creo nuovo file
                    path_cedolini='{0}/output/cedolini'.format(path)
                    path_anno='{0}/{1}'.format(path_cedolini, anno)
                    if not os.path.exists(path_anno):
                        os.makedirs(path_anno)
                    path_mese='{0}/{1}'.format(path_anno, mese)
                    if not os.path.exists(path_mese):
                        os.makedirs(path_mese)
                    
                    outputpdf='{0}/{1}-{2}-{3}-{4}--LUL1--{5}.pdf'.format(path_mese,CF_AZIENDA, CF, anno,mese, matricola)
                    if os.path.isfile(outputpdf):
                        outputpdf='{0}/{1}-{2}-{3}-{4}--LUL1--{5}_bis.pdf'.format(path_mese,CF_AZIENDA, CF, anno,mese, matricola)
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


    aa=0
    f2 = open('{}/{}'.format(path, file_anomalie), "a")
    while aa<len(a_file):
        f2.write('cedolini;{};{};{};{}\n'.format(a_anno[aa], a_mese[aa], a_CF[aa], a_file[aa]))
        aa+=1
    f2.close()


if __name__ == "__main__":
    main()       