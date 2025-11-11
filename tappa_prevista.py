#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2023
# Roberto Marzocchi, Roberta Fagandini



def tappa_prevista(day,frequenza_binaria):
    '''
    Data una data e una frequenza dice se la tappa è prevista sulla base di quella frequenza o no

    ############################## ATTENZIONE ################################
        da aggiungere frequenza settimanale per gestire percorsi bisettimanali
    ##########################################################################
    '''
    # settimanale
    if frequenza_binaria[0]=='S':
        if int(frequenza_binaria[day.weekday()+1])==1:
            return 1
        elif int(frequenza_binaria[day.weekday()+1])==0:
            return -1
        else:
            return 404
    # mensile (da finire)
    elif frequenza_binaria[0]=='M':
        # calcolo la settimana (week_number) e il giorno della settimana (day of week --> dow)
        if (day.day % 7)==0:
            week_number = ((day.day) // 7)
        else:     
            week_number = ((day.day) // 7) + 1
        dow=day.weekday()+1
        string='{0}{1}'.format(week_number,dow)
        # verifico se il giorno sia previsto o meno
        if string in frequenza_binaria:
            return 1
        else: 
            return -1