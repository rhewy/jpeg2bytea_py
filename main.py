# ==============================================================================
# File             : main.py
#
# Current Author   : Robert Hewlett
#
# Previous Author  : None
#
# Contact Info     : rob.hewy@gmail.com
#
# Purpose          : Insert some blobs (jpegs) in a bytea column in postgresql
#
# Dependencies     : psycopg2 --> python pg lib --> conda install psycopg2
#                    in postgres a table called parcels with a bytea column named map
#                    see pg_backup\parcels.backup
#
# Modification Log :
#    --> Created 2020-10-30 (rh)
#    --> Updated YYYY-MM-DD (fl)
#
# =============================================================================
import getpass as gp
import os as o
import traceback as tb

import psycopg2 as pg

# ======================
# Constants and defaults
# ======================
imagesDirName = 'images'
dotChar = '.'
seperatorChar = '_'
# ===================================================
# DB defaults you will probably have to change these
# ====================================================
table = 'parcels'
schema = 'gist_7132_m07'
host = 'gamma.athena.bcit.ca'
port = 5432
# ================================
# Get the user name and password
# ================================
userName = input('Input your user name: ')
# ============================================
# Your db may not be the same as your username
# ============================================
dbname = userName
password = gp.getpass('Input your password: ')
# ==========================================
# Build up the connection string
# ==========================================
connString = f'host={host} dbname={dbname} user={userName} password={password}'

parcel_id = None

try:
    with pg.connect(connString) as conn:
        # =================================
        # Use a cursor to do an update SOP
        # =================================
        with conn.cursor() as cur:
            # ==========================================================================
            # For every file in the image directory update the blob field in the table
            # ==========================================================================
            for file in o.listdir(imagesDirName):
                # ========================================
                # Slice the parcel id from the file name
                # Nasty but it works
                # ========================================
                parcel_id = file[file.index(seperatorChar)+1:file.index(dotChar)]
                # ========================================
                # Template SQL for the update
                # %s will be swapped for a big-chunk of bytes
                # ========================================
                sql = f'UPDATE {schema}.{table} SET map = %s WHERE parcels_id = {parcel_id}'
                print(sql)
                # ========================================
                # Open the file in binary mode
                # ========================================
                with open(o.path.join(imagesDirName, file), 'rb') as blobFile:
                    # ========================================
                    # execute the SQL but swap in the file for %s
                    # could have several values to swap so
                    # the default is a list or tuple to swap in
                    # Convert the python binary stream to a pg blob
                    # ========================================
                    cur.execute(sql, [pg.Binary(blobFile.read())])
except Exception as e1:
    tb.print_exc()

