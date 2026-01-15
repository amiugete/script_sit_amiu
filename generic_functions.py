#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2026
# Roberto Marzocchi Roberta FAgandini

def get_fascia_turno(ora_inizio_lav, ora_fine_lav, ora_inizio_lav_2 ,ora_fine_lav_2):
    '''
    Calcolo della fascia turno sulla base degli orari della scheda di lavoro Ekovision
    '''
    fascia_turno=''
    if ora_inizio_lav_2 == '000000' and ora_fine_lav_2 =='000000':
    
        if ora_inizio_lav== '000000' and ora_fine_lav =='000000':
            fascia_turno='D'
        else:
            oi=int(ora_inizio_lav[:2])
            mi=int(ora_inizio_lav[2:4])
            of=int(ora_fine_lav[:2])
            mf=int(ora_fine_lav[2:4])
    else:
        oi=int(ora_inizio_lav[:2])
        mi=int(ora_inizio_lav[2:4])
        of=int(ora_fine_lav_2[:2])
        mf=int(ora_fine_lav_2[2:4])
            
            
    if fascia_turno=='':        
        # calcolo minuti del turno
        if of < oi:
            minuti= 60*(24 - oi) + 60 * of - mi + mf
        else :
            minuti = 60 * (of-oi) - mi + mf 

        
        hh_plus=int(minuti/2/60)
        mm_plus=minuti/2-60*int(minuti/2/60)
        
        # ora media
        if mi+mm_plus >= 60:
            mm=mi+mm_plus-60
            hh=oi+1+hh_plus
        else:
            mm=mi+mm_plus
            hh=oi+hh_plus
        
        #print('{}:{}'.format(hh,mm))
        
        if hh > 5 and hh <= 12:
            fascia_turno = 'M'
        elif hh > 12 and hh <= 20:
            fascia_turno = 'P'
        elif hh > 20 or hh <= 5:
            fascia_turno= 'N'
        
    return fascia_turno