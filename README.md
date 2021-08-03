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