#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2021
# Roberto Marzocchi

'''
Funzioni per inviare mail e aggiungere allegati usate dentro altri script
'''


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

from credenziali import *

def invio_messaggio(messaggio):

    # Create a secure SSL context
    context = ssl.create_default_context()

    check=0
    # questo funzionava con GMAIL ma non va bene con SERVER AMIU
    try:
        with smtplib.SMTP_SSL(smtp_mail, port_mail, context=context) as server:
            server.login(user_mail, pwd_mail)
            server.send_message(messaggio)
            #server.sendmail(user_mail, receiver_email, text)
        check=200
    except Exception as e:
        e1=e 
    
    if check==0:
        # questo dovrebbe  funzionare con SERVER AMIU INTERNO
        try:
            server = smtplib.SMTP(smtp_mail,port_mail)
            server.ehlo() # Can be omitted
            #server.starttls(context=context) # Secure the connection
            #server.ehlo() # Can be omitted
            #server.login(user_mail, pwd_mail)
            #server.sendmail(user_mail, receiver_email, text)
            server.send_message(messaggio)
            check=200
        except Exception as e:
            # Print any error messages to stdout
            e2=e
    
    if check==200: 
        return check
    else:
        check='500 - ERRORI: {} e {}'.format(e1,e2)

    return check



def allegato(messaggio, file, nome_file):
    ctype, encoding = mimetypes.guess_type(file)
    if ctype is None or encoding is not None:
        ctype = "application/octet-stream"

    maintype, subtype = ctype.split("/", 1)

    if maintype == "text":
        fp = open(file)
        # Note: we should handle calculating the charset
        attachment = MIMEText(fp.read(), _subtype=subtype)
        fp.close()
    elif maintype == "image":
        fp = open(file, "rb")
        attachment = MIMEImage(fp.read(), _subtype=subtype)
        fp.close()
    elif maintype == "audio":
        fp = open(file, "rb")
        attachment = MIMEAudio(fp.read(), _subtype=subtype)
        fp.close()
    else:
        fp = open(file, "rb")
        attachment = MIMEBase(maintype, subtype)
        attachment.set_payload(fp.read())
        fp.close()
        encoders.encode_base64(attachment)
    attachment.add_header("Content-Disposition", "attachment", filename=nome_file)
    messaggio.attach(attachment)