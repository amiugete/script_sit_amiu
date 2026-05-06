#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2026
# Roberto Marzocchi, Roberta Fagandini

'''
Lo script fa un ciclo sui 38 comuni gestiti da AMIU

crea 2 fogli sullo stesso file excel: 
- spazzamento
- raccolta



'''
# libreria per invio mail
import inspect
import logging
import os 
import sys

from datetime import date

import psycopg2
import psycopg2.extras
import xlsxwriter
from dateutil.relativedelta import relativedelta

from credenziali import *
from invio_messaggio import *

# ── Aggregazione spazzamento per (comune, anno, mese) 
from collections import defaultdict

# File report arera
OUTPUT_FILE = "indicatori_ARERA.xlsx"

#Titoli colonne file excel
TITOLI= [
    'Comune',
    'Anno',
    'Mese', 
    'Pianificati',
    'Effettuati', 
    'Forza maggiore',
    'Imputabili all\'utente',
    'Imputabili al gestore',
    'Indicatore puntualità'
]

# Causali ARERA per spazzamento e raccolta
RESULT_QUERY =[
               "Effettuato",
               "Cause di forza maggiore", 
               "Cause imputabili all'utente", 
                "Cause imputabili al gestore"
                ]

class Spazzamento():
    def __init__(self, comune=None, anno=None, mese=None, causale_arera=None, interruzione=None, round=None):
        # attributi espliciti per chiarezza
        self.comune = comune
        self.anno = anno
        self.mese = mese
        self.causale_arera = causale_arera
        self.interruzione = interruzione
        self.round = round

class Raccolta():
    def __init__(self, comune=None, anno=None, mese=None, causale_arera=None, interruzione=None, count=None):
        self.comune = comune
        self.anno = anno
        self.mese = mese
        self.causale_arera = causale_arera
        self.interruzione = interruzione
        self.count = count



# ─── QUERY  COMUNI ────────────────────────────────────────────────────────────────────

query_comuni= """
    select * from topo.comuni c 
    where c.gestito_sit = 'S'
    order by 2
    """

# ─── QUERY  RACCOLTA ────────────────────────────────────────────────────────────────────
query_raccolta ="""
with diss_s0 as (
    select 
    c.descr_comune as comune,
    case 
	    when ep.giorno_competenza = 0 then extract(year from data_programmata)
	    when ep.giorno_competenza = -1 then extract(year from data_programmata-1)
    end anno,
    case 
        when ep.giorno_competenza = 0 then extract(month from data_programmata)
        when ep.giorno_competenza = -1 then extract(month from data_programmata-1)
    end mese, 
    case 
        when (tipo_raccolta = 'OTH' and tempo_recupero > 72)
        or 
        (tipo_raccolta in ('DOM', 'PRG') and tempo_recupero > 24)
        then 1
        else 0
    end interruzione,
    case 
        when tempo_ripresa >= 24
        then 1
        else 0
    end disservizio,
    trac_code, 
    cd.codice,
    cd.descrizione as causale,
    ca.id as id_causale_arera,
    ca.descrizione as causale_arera
    from consunt.report_raccolta rr
    join topo.vie v on v.id_via = rr.id_via
    join topo.comuni c on c.id_comune= v.id_comune
    join anagrafe_percorsi.elenco_percorsi ep on ep.cod_percorso = rr.cod_percorso
        and data_programmata between ep.data_inizio_validita and ep.data_fine_validita - 1
    join etl.cause_disserv cd on cd.codice = rr.id_causale
    left join etl.causali_arera ca on cd.id_causale_arera = ca.id
    join anagrafe_percorsi.anagrafe_tipo at2 on at2.id = ep.id_tipo
    where rr.non_previsto is null
    and at2.gestione_arera = true 
    and rr.id_causale not in (101,102, 999)
    and c.id_comune = %s
),
diss_s1 as (
	select trac_code, comune, anno, mese,  min(interruzione) as interruzione,
	case 
		when min(interruzione) = 0 then null
		else max(id_causale_arera)
	end id_causale_arera 
	from  diss_s0 
	group by comune, anno, mese, interruzione, trac_code
)
select comune, anno, mese,  ca.descrizione as causale_arera, interruzione,
count(distinct trac_code)
from  diss_s1 a
left join etl.causali_arera ca on a.id_causale_arera = ca.id 
where  interruzione = 1
group by comune, anno, mese, causale_arera, interruzione 
union 
select comune, anno, mese,  'Effetuato' as causale_arera, 0 as interruzione,
count(distinct trac_code)
from  diss_s1 a
where  interruzione = 0
group by comune, anno, mese
order by anno, mese

"""





# ─── QUERY  SPAZZAMENTO ────────────────────────────────────────────────────────────────────
query_spazzamento = """with diss_s0 as (
select 
c.descr_comune as comune,
case 
	    when ep.giorno_competenza = 0 then extract(year from data_programmata)
	    when ep.giorno_competenza = -1 then extract(year from data_programmata-1)
    end anno,
    case 
        when ep.giorno_competenza = 0 then extract(month from data_programmata)
        when ep.giorno_competenza = -1 then extract(month from data_programmata-1)
    end mese, 
case 
	when min(rr.tempo_recupero) > 24
	then 1
	else 0
end interruzione,
case 
	when min(tempo_ripresa) >= 24
	then 1
	else 0
end disservizio,
trac_code, 
min(rr.lung_km) as lung_km, 
min(cd.codice) as codice,
min(cd.descrizione) as causale,
max(cd.id_causale_arera) as id_causale_arera
from consunt.report_spazz rr
join topo.vie v on v.id_via = rr.id_via
join topo.comuni c on c.id_comune= v.id_comune
join anagrafe_percorsi.elenco_percorsi ep on ep.cod_percorso = rr.cod_percorso
	and data_programmata between ep.data_inizio_validita and ep.data_fine_validita - 1
join etl.cause_disserv cd on cd.codice = rr.id_causale
--left join etl.causali_arera ca on cd.id_causale_arera = ca.id
join anagrafe_percorsi.anagrafe_tipo at2 on at2.id = ep.id_tipo
where rr.non_previsto is null
and at2.gestione_arera = true 
and rr.id_causale not in (101,102,999)
/*and data_programmata between to_date('20250101', 'YYYYMMDD') and to_date('20250131', 'YYYYMMDD')*/
and c.id_comune = %s
group by
c.descr_comune, 
data_programmata, 
ep.giorno_competenza,
trac_code
), 
diss_s1 as (
	select trac_code, comune, anno, mese,  min(interruzione) as interruzione,
	case 
		when min(interruzione) = 0 then null
		else max(id_causale_arera)
	end id_causale_arera,
    min(lung_km) as lung_km
	from  diss_s0 
    group by comune, anno, mese, trac_code
)
select comune, anno, mese,  ca.descrizione as causale_arera, interruzione,
round(sum(round(lung_km,3)),3)
from  diss_s1 d
left join etl.causali_arera ca on d.id_causale_arera = ca.id
where  interruzione = 1
group by comune, anno, mese, causale_arera, interruzione 
union 
select comune, anno, mese,  'Effetuato' as causale_arera, 0 as interruzione,
round(sum(round(lung_km,3)),3)
from  diss_s1 
where  interruzione = 0
group by comune, anno, mese 
order by anno, mese"""



def main():
    """
    il calcolo dei pianificati è la somma per ogni mese dei 4 (mettendo a 0 quello che non hai)
    il calcolo dell'indicatore puntualità è dato da:
    1 - Imputabili al gestore / Pianificati * 100
    """
    filename = inspect.getframeinfo(inspect.currentframe()).filename
    path     = os.path.dirname(os.path.abspath(filename))
    path=os.path.dirname(sys.argv[0]) 
    nome=os.path.basename(__file__).replace('.py','')

    logfile='{0}/log/{1}.log'.format(path,nome)
    errorfile='{0}/log/error_{1}.log'.format(path,nome)


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
    oggi = date.today()
    inizio = oggi.replace(day=1) - relativedelta(months=2)
    mesi = [inizio + relativedelta(months=i) for i in range(2)]
    

    # lista spazzamento e raccolta
    lista_spazzamenti : list[Spazzamento] = []
    lista_raccolta : list[Raccolta] = []
    
    # Mi connetto a SIT (PostgreSQL) usando context manager per chiusure automatiche
    try:
        nome_db = db
        logger.info('Connessione al db {}'.format(nome_db))
        with psycopg2.connect(dbname=nome_db,
                              port=port,
                              user=user,
                              password=pwd,
                              host=host) as conn:
                logger.info('Connessione al db {} riuscita'.format(nome_db))
                # QUERY COMUNI
                with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as curr:
                    curr.execute(query_comuni)
                    lista_comuni = curr.fetchall()

                    # CICLO COMUNI
                    for row in lista_comuni:
                        id_comune = row['id_comune']
                        desc_comune = row['descr_comune']

                        # QUERY SPAZZAMNENTO
                        curr.execute(query_spazzamento, (id_comune,))
                        rows_spazz = curr.fetchall()

                        logger.info(f'Inizio ciclo spazzamento comune {desc_comune} .....')
                        for row in rows_spazz:
                            spazzamento = Spazzamento(
                                row['comune'],
                                row['anno'],
                                row['mese'],
                                row['causale_arera'],
                                row['interruzione'],
                                row['round']
                            )
                            lista_spazzamenti.append(spazzamento)
                        logger.info(f'Fine ciclo spazzamento comune {desc_comune} .....')

                        # QUERY RACCOLTA
                        curr.execute(query_raccolta, (id_comune,))
                        rows_raccolta = curr.fetchall()

                        logger.info(f'Inizio ciclo raccolta comune {desc_comune} .....')
                        for row in rows_raccolta:
                            raccolta = Raccolta(
                                row['comune'],
                                row['anno'],
                                row['mese'],
                                row['causale_arera'],
                                row['interruzione'],
                                row['count']
                            )
                            lista_raccolta.append(raccolta)
                        logger.info(f'Fine ciclo raccolta comune {desc_comune} .....')
    except psycopg2.Error as e:
        logger.error(e)
        logger.error("Errore durante il recupero dei dati dal database", exc_info=True)
        logger.error(f"Errore specifico del database: {e.pgerror}")
        logger.error(f"Codice SQLSTATE: {e.pgcode}")
        error_log_mail(errorfile,
                       'roberto.marzocchi@amiu.genova.it, roberta.fagandini@amiu.genova.it, richard.moschini@amiu.genova.it',
                       os.path.basename(__file__),
                       logger)
        exit(1)
    except Exception as e:
        tb = sys.exc_info()[2]
        logger.error('Errore: {} alla riga {} dello script'.format(e, tb.tb_lineno))
        error_log_mail(errorfile,
                       'roberto.marzocchi@amiu.genova.it, roberta.fagandini@amiu.genova.it, richard.moschini@amiu.genova.it',
                       os.path.basename(__file__),
                       logger)
        exit(1)



    # faccio un mega try per essere sicuro che l'indice venga rimosso alla fine dell'elaborazione anche in caso di errori a metà processo
    try:
        ############################## MANIPOLAZIONE DATI E SCRITTURA EXCEL ##############################
        
        # Raggruppo i dati grezzi per chiave (comune, anno, mese)
        agg_spazz = defaultdict(lambda: {
            'Effetuato': 0,
            'Cause di forza maggiore': 0,
            'Cause imputabili all\'utente': 0,
            'Cause imputabili al gestore': 0,
        })

        for s in lista_spazzamenti:
            chiave = (s.comune, s.anno, s.mese)
            agg_spazz[chiave][s.causale_arera] = float(s.round or 0)

        # ── Aggregazione raccolta per (comune, anno, mese) ──────────────────────

        agg_racc = defaultdict(lambda: {
            'Effetuato': 0,
            'Cause di forza maggiore': 0,
            'Cause imputabili all\'utente': 0,
            'Cause imputabili al gestore': 0,
        })

        for r in lista_raccolta:
            chiave = (r.comune, r.anno, r.mese)
            agg_racc[chiave][r.causale_arera] = float(r.count or 0)

        # ── Scrittura Excel ─────────────────────────────────────────────────────────
        workbook = xlsxwriter.Workbook(OUTPUT_FILE)

        # Formati
        header_fmt = workbook.add_format({'bold': True, 'bg_color': '#4472C4', 'font_color': 'white', 'border': 1})
        num_fmt = workbook.add_format({'num_format': '#,##0.000', 'border': 1})
        pct_fmt = workbook.add_format({'num_format': '0.00"%"', 'border': 1})
        txt_fmt = workbook.add_format({'border': 1})

        w = workbook.add_worksheet('Spazzamento')

        # Intestazioni
        for col, titolo in enumerate(TITOLI):
            w.write(0, col, titolo, header_fmt)

        riga = 1
        for (comune, anno, mese) in sorted(agg_spazz.keys()):
            vals = agg_spazz[(comune, anno, mese)]
            effettuati = vals['Effetuato']
            forza_maggiore = vals['Cause di forza maggiore']
            imp_utente = vals['Cause imputabili all\'utente']
            imp_gestore = vals['Cause imputabili al gestore']
            pianificati = effettuati + forza_maggiore + imp_utente + imp_gestore
            indicatore = (1 - imp_gestore / pianificati) * 100 if pianificati else 0

            w.write(riga, 0, comune, txt_fmt)
            w.write(riga, 1, int(anno), txt_fmt)
            w.write(riga, 2, int(mese), txt_fmt)
            w.write(riga, 3, pianificati, num_fmt)
            w.write(riga, 4, effettuati, num_fmt)
            w.write(riga, 5, forza_maggiore, num_fmt)
            w.write(riga, 6, imp_utente, num_fmt)
            w.write(riga, 7, imp_gestore, num_fmt)
            w.write(riga, 8, indicatore, pct_fmt)
            riga += 1

        # Auto-larghezza colonne
        for col, titolo in enumerate(TITOLI):
            w.set_column(col, col, max(len(titolo) + 2, 14))

        w.autofilter(0, 0, riga - 1, len(TITOLI) - 1)

        # ── Scrittura sheet Raccolta ───────────────────────────────────────────
        w2 = workbook.add_worksheet('Raccolta')

        # Intestazioni raccolta
        for col, titolo in enumerate(TITOLI):
            w2.write(0, col, titolo, header_fmt)

        riga2 = 1
        for (comune, anno, mese) in sorted(agg_racc.keys()):
            vals = agg_racc[(comune, anno, mese)]
            effettuati = vals['Effetuato']
            forza_maggiore = vals['Cause di forza maggiore']
            imp_utente = vals['Cause imputabili all\'utente']
            imp_gestore = vals['Cause imputabili al gestore']
            pianificati = effettuati + forza_maggiore + imp_utente + imp_gestore
            indicatore = (1 - imp_gestore / pianificati) * 100 if pianificati else 0

            w2.write(riga2, 0, comune, txt_fmt)
            w2.write(riga2, 1, int(anno), txt_fmt)
            w2.write(riga2, 2, int(mese), txt_fmt)
            w2.write(riga2, 3, pianificati, num_fmt)
            w2.write(riga2, 4, effettuati, num_fmt)
            w2.write(riga2, 5, forza_maggiore, num_fmt)
            w2.write(riga2, 6, imp_utente, num_fmt)
            w2.write(riga2, 7, imp_gestore, num_fmt)
            w2.write(riga2, 8, indicatore, pct_fmt)
            riga2 += 1

        # Auto-larghezza colonne raccolta
        for col, titolo in enumerate(TITOLI):
            w2.set_column(col, col, max(len(titolo) + 2, 14))

        w2.autofilter(0, 0, riga2 - 1, len(TITOLI) - 1)
        workbook.close()

        logger.info(f'File {OUTPUT_FILE} creato con {riga - 1} righe di spazzamento e {riga2 - 1} righe di raccolta.')
    except Exception as e:
        tb = sys.exc_info()[2]
        logger.error('Errore durante l\'elaborazione dei dati')
        logger.error('Errore: {} alla riga {} dello script'.format(e, tb.tb_lineno))
        error_log_mail(errorfile, 'roberto.marzocchi@amiu.genova.it, richard.moschini@amiu.genova.it', os.path.basename(__file__), logger)

if __name__ == "__main__":
    main()  
