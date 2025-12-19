#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2025
# Roberto Marzocchi Roberta Fagandini


''''
ATTENZIONE: 

In caso di modifiche allo script bisogna compilarlo di nuovo con pyinstaller:

ATTTENZIONE pyinstaller non è cross-platform, va eseguito sullo stesso sistema operativo su cui si vuole eseguire il file compilato.

# questo crea unico file
python -m PyInstaller --clean --onefile process_excel_bilaterale.py

# questo cartella con dipendenze
python -m PyInstaller --clean --onefile process_excel_bilaterale.py

'''


import pandas as pd
import inspect
import os
#import numpy as np
import sys
import logging


path=os.path.dirname(sys.argv[0]) 
nome=os.path.basename(__file__).replace('.py','')
#tmpfolder=tempfile.gettempdir() # get the current temporary directory
logfile='{0}/{1}.log'.format(path,nome)
errorfile='{0}/error_{1}.log'.format(path,nome)




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

    logger.info('Inizio a cercare i file nella cartella')
    for file in os.listdir(path):
        if file.endswith('.xlsx') and file.startswith('report_bilaterali'):
            try:
                logger.info('Converto il file: {}'.format(file))

                file_input = file
                xls = pd.ExcelFile(file_input)

                lista_df = []

                for nome_foglio in xls.sheet_names:
                    # Legge i dati (intestazioni alla riga 6)
                    df = pd.read_excel(
                        file_input,
                        sheet_name=nome_foglio,
                        header=5
                    )

                    # Elimina eventuali righe completamente vuote
                    df = df.dropna(how="all")

                    # Crea la sequenza che riparte da 1 per ogni foglio
                    df.insert(0, "Sequenza", range(1, len(df) + 1))

                    # Legge Codice e Descrizione (riga 1)
                    meta = pd.read_excel(
                        file_input,
                        sheet_name=nome_foglio,
                        header=None,
                        nrows=1,
                        dtype=str
                    )

                    codice = str(meta.iloc[0, 1])        # B1
                    descrizione = str(meta.iloc[0, 5])   # F1

                    # Aggiunge Codice e Descrizione
                    df["Codice"] = codice
                    df["Descrizione"] = descrizione

                    lista_df.append(df)

                # Unisce tutti i fogli
                df_finale = pd.concat(lista_df, ignore_index=True)

                # Esporta il file finale
                file_output = "{0}/unico_{1}".format(path,file)
                df_finale.to_excel(file_output, index=False)
            except Exception as e:
                logger.error(e)
        else:
            logger.warning('Il file {} non è un file di report bilaterali, passo al successivo'.format(file))
    
    logger.info('Ho processato tutti i file nella cartella, chiudo lo script.')

if __name__ == "__main__":
    main() 