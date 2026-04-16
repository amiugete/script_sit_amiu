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
        


################################################################################
# TODO 
# RINOMINARE IL FILE .PY IN MODO PIU' FURBO (es. env_script_sit.py)
#################################################################################
        
        
def decode_turno (cursor, id_turno, logger):
    '''
    Dato un id_turno recupera (per ora) il solo orario in maniera descrittiva
    '''
    
    sql='''
    select concat(
            lpad(t.inizio_ora::text,2,'0'), ':', lpad(t.inizio_minuti::text,2,'0'),
            ' - ',
            lpad(t.fine_ora::text,2,'0'), ':', lpad(t.fine_minuti::text,2,'0')) as orario
from elem.turni t
where id_turno = %s
    '''
    
    try: 
        cursor.execute(sql, (id_turno,))
        riga=cursor.fetchone()
    except Exception as e:
        logger.error(sql)
        logger.error(e)

    if riga:
        return riga[0]
    else:
        logger.warning(f'Non trovo id_turno {id_turno}')
        return None
    
    
    
def get_asta_civ_rif(cursor, id_elemento, logger):
    
    '''
    Input id_elemento 
    
    Output 
     - 0 id_asta
     - 1 rif
     - 2 civ
    
    '''
    
    
    sql_piazzola = '''
    select id_elemento, id_piazzola from elem.elementi e 
where id_elemento = %s
union 
select id_elemento, id_piazzola from history.elementi e 
where id_elemento = %s
    '''
    
    
    try:
        cursor.execute(sql_piazzola, (id_elemento, id_elemento,))
        row_piazzola=cursor.fetchone()
    except Exception as e:
        logger.error(sql_piazzola)
        logger.error(e)
        
    
    
    # cerco asta, civico e rif

    select_from_p = '''SELECT id_asta, numero_civico, riferimento
            FROM elem.piazzole
            WHERE id_piazzola = %s'''
            
    select_from_e = '''SELECT id_asta, numero_civico, riferimento
            FROM elem.elementi
            WHERE id_elemento = %s
            union 
            SELECT id_asta, numero_civico, riferimento
            FROM history.elementi
            WHERE id_elemento = %s
        '''

    
    
    
    
    
    if row_piazzola[1] is None:
        try:
            cursor.execute(select_from_e, (row_piazzola[0],row_piazzola[0]))
            row_rif=cursor.fetchone()
        except Exception as e:
            logger.error(select_from_e)
            logger.error(e)
    else:
        try:
            cursor.execute(select_from_p, (row_piazzola[1],))
            row_rif=cursor.fetchone()
        except Exception as e:
            logger.error(select_from_p)
            logger.error(e)
    
    
    if row_rif:
        return row_rif

        