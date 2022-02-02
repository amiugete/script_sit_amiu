#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2021
# Roberto Marzocchi

'''
Lo script esporta tabelle da SIT PROG a un geopackage locale
'''


import os
from credenziali import *

from osgeo import ogr, gdal

PG_CONN = {
    'user': os.getenv(user),
    'password': os.getenv(pwd),
    'port': os.getenv(port),
    'host': os.getenv(host),
    'db': os.getenv(db_prog),
}

def pg_to_gpkg(tablename):
    """Processes the requests to generate geopackages
    Args:
        tablename (text): table id
    """

    connString = f"PG: db='{PG_CONN['db']}' host='{PG_CONN['host']}' user='{PG_CONN['user']}' password='{PG_CONN['password']}'"

    conn = ogr.Open(connString)

    gdal.SetConfigOption('PG_USE_COPY', 'YES')

    print(f"Processing {tablename}")

    # # get the data
    sql = f"SELECT * FROM {tablename}"
    ogr_lyr = conn.ExecuteSQL(sql)
    print(f"Found {len(ogr_lyr)} elements")

    # # generate filename and dir
    curdir = os.path.dirname(os.path.abspath(__file__))
    exportdir = os.path.join(curdir, 'export')
    filename = f"{tablename}.gpkg"

    if not os.path.exists(exportdir):
        os.makedirs(exportdir)

    out_file = os.path.join(exportdir, filename)

    # # generate the geopackage
    _options = gdal.VectorTranslateOptions(
        **{'layerName': tablename, 'SQLStatement': sql, 'format': 'GPKG'}
    )

    if len(ogr_lyr) > 0:
        if os.path.exists(out_file):
            os.remove(out_file)
        src_ds = gdal.OpenEx(connString, gdal.OF_VECTOR)
        gdal.VectorTranslate(out_file, src_ds, options=_options)
    return


def main():
    pg_to_gpkg('''geo.v_piazzole_geom''')

if __name__ == "__main__":
    main()  
