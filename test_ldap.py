#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2021
# Roberto Marzocchi



import sys,ldap,ldap.asyncsearch
from credenziali import *

user1='porta'
#Jessica.Raveendrarasa@amiu.genova.it

try:


    connect = ldap.initialize(ldap_url)
    #connect = ldap.initialize("ldaps://amiu.genova.it:636")
    connect.set_option(ldap.OPT_REFERRALS, 0)
    
    connect.set_option(ldap.OPT_PROTOCOL_VERSION, 3)
    connect.set_option(ldap.OPT_X_TLS,ldap.OPT_X_TLS_DEMAND)
    connect.set_option(ldap.OPT_X_TLS_DEMAND, True)
    connect.set_option(ldap.OPT_DEBUG_LEVEL, 255)
    # This must be the last tls setting to create TLS context.
    #connect.set_option(ldap.OPT_X_TLS_NEWCTX, ldap.OPT_ON)

    
    '''
    connect = ldap.initialize('ldap://amiu.genova.it:389')
    #connect = ldap.initialize('ldap://login.microsoftonline.com')
    connect.set_option(ldap.OPT_REFERRALS, 0)
    '''
    
    connect.simple_bind_s('{}@amiu.genova.it'.format(ldap_login), ldap_pwd)
    criteria = "(&(objectClass=user)(sAMAccountName={0}))".format(user1)
    attributes = ['sAMAccountName', 'mail', 'uid', 'userprincipalname']
    result = connect.search_s('DC=amiu,DC=genova,DC=it',
                        ldap.SCOPE_SUBTREE, criteria, attributes)
    print(result)
    sAn=result[0][1]['sAMAccountName'][0].decode('utf-8')
    print(sAn)
    mail=result[0][1]['mail'][0].decode('utf-8')
except Exception as e:
  print(e)



exit()
s = ldap.asyncsearch.List(
  ldap.initialize(ldap_url),
)

s.startSearch(
  'dc=stroeder,dc=com',
  ldap.SCOPE_SUBTREE,
  '(objectClass=*)',
)

try:
  partial = s.processResults()
except ldap.SIZELIMIT_EXCEEDED:
  sys.stderr.write('Warning: Server-side size limit exceeded.\n')
else:
  if partial:
    sys.stderr.write('Warning: Only partial results received.\n')

sys.stdout.write(
  '%d results received.\n' % (
    len(s.allResults)
  )
)