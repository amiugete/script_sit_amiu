

CREATE SCHEMA spazz_schmidt AUTHORIZATION gisamiu;



CREATE TABLE spazz_schmidt.serialnumbers (
	id int NOT NULL,
	manuf_id varchar NULL,
	equip_id varchar NULL,
	CONSTRAINT serialnumbers_pk PRIMARY KEY (id)
);

CREATE TABLE spazz_schmidt.messaggi (
	gid serial not null,
	id int NOT NULL,
	last_valid_id int NOT NULL,
	serialnumber_id int NOT NULL,
	routeid varchar NULL,
	driverid varchar NULL,
	driver2id varchar NULL,
	geoloc public.geometry(point, 4326) NOT NULL,
	data_ora timestamp NULL,
	data_ora_inserimento timestamp NULL DEFAULT now(),
	sweeper_mode int NOT NULL,
	CONSTRAINT messaggi_pk PRIMARY KEY (gid),
    CONSTRAINT messaggi_fk FOREIGN KEY (serialnumber_id) REFERENCES spazz_schmidt.serialnumbers(id)
);