# script_sit_amiu
Script utili per il DB SIT di Amiu


## credenziali 
Le credenziali devono essere inserite in un file **credenziali.py** con il seguente formato:

db='nome_db'
port=5432 # or different port
user='username'
pwd='password'
host='server_host or IP'


## Gestione dei permessi

sudo chown procedure:www-data -R ./*
sudo chmod 775 log
sudo chmod 775 utenze

## CRONTAB

Quasi tutti gli script di questa repository sono schedulati e lanciati tramite crontab (/etc/crontab) su server amiugis

Il file crontab viene monitorato per segnalare eventuali errori nella struttura o nella sintassi da due file:
 - /usr/local/bin/controllo_crontab.sh
 - /usr/local/bin/valida_crontab.sh

controllo_crontab.sh:
  - crea backup del file crontab se è stato modificato
  - lancia il lo script valida_crontab.sh
  - invia mail di notifica di eventuali errori

valida_crontab.sh:
  - verifica errori strutturali del file (es. se le righe sono composte da tutti gli elementi necessari)
  - verifica errori di sintassi (per questo è stato necessario usare la libreria python croniter dal momento che i controlli di sintassi standard sono validi solo per il crontab dell'utente che noi non usiamo)

**NB.** Su crontab di root è schedulato ogni 10 minuti lo script controllo_crontab.sh
