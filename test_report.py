#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2021
# Roberto Marzocchi



import sys,ldap,ldap.asyncsearch
from credenziali import *
import report_settimanali_percorsi_ok 

def main():
  
  report_settimanali_percorsi_ok.main('0111003703', 'sempl', 'roberto.marzocchi@gmail.com', 1)
  
  

if __name__ == "__main__":
    main()  