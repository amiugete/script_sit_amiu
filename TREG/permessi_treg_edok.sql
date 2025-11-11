--create schema treg_edok;


/*2.a) Connessione al DB*/
--GRANT CONNECT ON DATABASE database TO user;
/*2.b) Possibilità di usare uno schema specifico*/
GRANT USAGE ON schema treg_edok to postgres;
GRANT USAGE ON SCHEMA treg_edok TO webgis;
GRANT USAGE ON SCHEMA treg_edok TO gisamiu;
GRANT CREATE ON SCHEMA treg_edok TO gisamiu;


/*2.c) Possibilità di far qualcosa su tutte le tabelle di quello schema*/
GRANT ALL ON ALL TABLES IN SCHEMA treg_edok TO postgres;
GRANT select, update, insert, delete, trigger ON ALL TABLES IN SCHEMA treg_edok TO gisamiu;
GRANT SELECT ON ALL TABLES IN SCHEMA treg_edok TO webgis;


/*2.d) Possibilità di far qualcosa su tutte le tabelle di quello schema (anche quelle
che verranno create/modificate da ora in avanti)
esempio*/
ALTER DEFAULT PRIVILEGES IN SCHEMA treg_edok
GRANT ALL ON TABLES TO postgres;

ALTER DEFAULT PRIVILEGES IN SCHEMA treg_edok
GRANT select, update, insert, delete, trigger ON TABLES TO gisamiu;

ALTER DEFAULT PRIVILEGES IN SCHEMA treg_edok
GRANT SELECT ON TABLES TO webgis;