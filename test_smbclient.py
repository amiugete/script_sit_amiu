#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2021
# Roberto Marzocchi



import sys,ldap,ldap.asyncsearch
from credenziali import *

import smbprotocol
from smbprotocol import *
from smbprotocol.connection import Connection, Dialects
from smbprotocol.session import Session
from smbprotocol.tree import TreeConnect
from smbprotocol.open import Open, FileAttributes, CreateDisposition, FilePipePrinterAccessMask


user1='marzocchi'



file_path=f'//{user1}/C$/Users/{user1}/Desktop/hoptodesk.exe'



# Inizializza smbprotocol
#smbprotocol.reset()  # pulisce eventuali sessioni precedenti

# Crea connessione
conn = Connection(server=user1, port=445, dialect=Dialects.SMB_3_1_1)
conn.connect()

# Crea sessione autenticata
session = Session(conn, username=ldap_login, password=ldap_pwd)
session.connect()

# Connetti alla share
tree = TreeConnect(session, fr"//{user1}\C$/Users/{user1}/Desktop/hoptodesk.exe")
tree.connect()

# Prova ad aprire il file in sola lettura
try:
    file = Open(tree, file_path,
                desired_access=FilePipePrinterAccessMask.FILE_READ_ATTRIBUTES,
                disposition=CreateDisposition.FILE_OPEN)
    file.create()
    print("✅ File esiste nella share SMB.")
    file.close()
except FileNotFoundError:
    print("❌ File non trovato.")
finally:
    tree.disconnect()
    session.disconnect()
    conn.disconnect()





exit()
