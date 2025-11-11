--create schema treg_eko;


/*2.a) Connessione al DB*/
--GRANT CONNECT ON DATABASE database TO user;
/*2.b) Possibilità di usare uno schema specifico*/
GRANT USAGE ON schema treg_eko to postgres;
GRANT USAGE ON SCHEMA treg_eko TO webgis;
GRANT CREATE USAGE ON SCHEMA treg_eko to gisamiu;


/*2.c) Possibilità di far qualcosa su tutte le tabelle di quello schema*/
GRANT ALL ON ALL TABLES IN SCHEMA treg_eko TO postgres;
GRANT select, update, insert, delete, trigger ON ALL TABLES IN SCHEMA treg_eko TO gisamiu;
GRANT SELECT ON ALL TABLES IN SCHEMA treg_eko TO webgis;


/*2.d) Possibilità di far qualcosa su tutte le tabelle di quello schema (anche quelle
che verranno create/modificate da ora in avanti)
esempio*/
ALTER DEFAULT PRIVILEGES IN SCHEMA treg_eko
GRANT ALL ON TABLES TO postgres;

ALTER DEFAULT PRIVILEGES IN SCHEMA treg_eko
GRANT select, update, insert, delete, trigger ON TABLES TO gisamiu;

ALTER DEFAULT PRIVILEGES IN SCHEMA treg_eko
GRANT SELECT ON TABLES TO webgis;
