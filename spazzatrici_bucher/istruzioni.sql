CREATE SCHEMA spazz_bucher AUTHORIZATION gisamiu;



CREATE TABLE spazz_bucher.mezzi (
	id int NOT NULL,
	sportello varchar NULL,
	sn varchar NOT NULL,
    note varchar NULL,
	CONSTRAINT mezzi_pk PRIMARY KEY (sn)
);

CREATE TABLE spazz_bucher.messaggi (
	gid serial not null,
	sportello varchar NOT NULL,
	routeid varchar NULL,
	driverid varchar NULL,
	geoloc public.geometry(point, 4326) NOT NULL,
	data_ora timestamp NULL,
	data_ora_inserimento timestamp NULL DEFAULT now(),
	sweeper_mode int NOT NULL,
	CONSTRAINT messaggi_pk PRIMARY KEY (gid),
    CONSTRAINT messaggi_fk FOREIGN KEY (sportello) REFERENCES spazz_bucher.mezzi(sportello)
);