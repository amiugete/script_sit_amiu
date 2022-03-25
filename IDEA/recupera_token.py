#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2021
# Roberto Marzocchi

'''
Lo script interroga i WS di IDEA

'''



import requests
from requests.exceptions import HTTPError





from credenziali import *



def token():
    api_url='{}/login'.format(url_idea)
    #print(api_url)
    response = requests.post(api_url, json=todo_idea)
    #response.json()
    #print(response.status_code)
    try:      
        response.raise_for_status()
        # access JSOn content
        #jsonResponse = response.json()
        #print("Entire JSON response")
        #print(jsonResponse)
    except HTTPError as http_err:
        print(f'HTTP error occurred: {http_err}')
    except Exception as err:
        print(f'Other error occurred: {err}')
        print(response.json())

    token = response.json()['token']
    #print(token)

    return(token)
