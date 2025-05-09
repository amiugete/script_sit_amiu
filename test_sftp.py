#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2021
# Roberto Marzocchi



import ftplib
import ssl


import pycurl

import os, sys

from credenziali import *



path=os.path.dirname(sys.argv[0]) 
nome=os.path.basename(__file__).replace('.py','')
#tmpfolder=tempfile.gettempdir() # get the current temporary directory
logfile='{0}/log/{1}.log'.format(path,nome)
errorfile='{0}/log/error_{1}.log'.format(path,nome)

ctx = ssl.create_default_context()
ctx.set_ciphers('DEFAULT:@SECLEVEL=1') # enables weaker ciphers and protocols


#acct = 'Normal'




#ret=os.system('''/command "open ftps://hosting2.mediscopio.com:231 -explicit -username=amiugenovaftp -password=SE!uz+esvfaBS5g%" "cd Import_AMIU" "put C:\filekettle\download_dati_inaz\inaz_output\Anagrafica_mediscopio.xlsx" "exit"''')


try:
  #ctx = ssl._create_stdlib_context(ssl.PROTOCOL_TLSv1_2)
  #ftps = ImplicitFTP_TLS()
  ftps = ftplib.FTP_TLS(context=ctx)
  
  #ftps.set_pasv(True)
  #ftps = ftplib.FTP_TLS(context=context1)
  
  #ftps.set_debuglevel(10)
  ftps.ssl_version = ssl.PROTOCOL_TLS
  ftps.connect(host_nike_nuovo, porta_nike_nuova, timeout=5)
  #ftps.prot_p()
  #ftps.auth()
  #ftps.port=porta_nike
  #print(ftps.getwelcome())
  #print(ftps.sock)

  #ftps.login(user_nike, pwd_nike, acct)
  ftps.login(user_nike, pwd_nike)
  ftps.prot_p()
except Exception as e:
  print('ERRORE: {}'.format(e))


print('Eccoci qua')

#exit()
try:
  ftps.set_pasv(True)
  filename="requirements.txt"
  #with open(filename, "rb") as file:
    # use FTP's STOR command to upload the file
  #  ftps.storbinary(f"STOR Import_AMIU/{filename}", file, 1024)
    #file.close()
  file = open(filename,'rb')                  # file to send
  print('letto file')
  ftps.storbinary(f'STOR Import_AMIU/{filename}', file, blocksize=8192, callback=None, rest=None)     # send the file
  #print('inviato file')
  file.close() 
  print('chiuso file')
  #list current files &amp; directories
  
except Exception as e:
  print('Problema invio file a nike')
  print(e)
  
ftps.dir()

# close file and FTP
ftps.quit()
exit()

