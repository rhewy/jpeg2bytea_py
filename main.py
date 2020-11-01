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
extChar = '.'
seperatorChar = '_'
# ===================================================
# DB defaults you will probably have to change these
# ====================================================
table = 'parcels'
schema = 'gist_7132_m07'
host = 'gamma.athena.bcit.ca'
port = 5432
# ========================================
# Template SQL for the update; the first
# %s will be swapped for a big-chunk of bytes
# the second %s will be the parcel_id
# ========================================
sqlWithParameters = f'UPDATE {schema}.{table} SET map = %s WHERE parcels_id = %s'
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

parcelId = None
try:
    with pg.connect(connString) as conn:
        # =================================
        # Use a cursor to do an update SOP
        # Note: we a recycling the cursor
        # =================================
        with conn.cursor() as cur:
            # ==========================================================================
            # For every file in the image directory update the blob field in the table
            # ==========================================================================
            for blobFile in o.listdir(imagesDirName):
                print(f'Processing image {blobFile}')
                # ========================================
                # Slice the parcel id from the file name
                # Nasty but it works (notice the +1)
                # pattern lot_N.jpg, lot_NN.jpg, lot_NNN.jpg
                # ========================================
                parcelId = blobFile[blobFile.index(seperatorChar)+1:blobFile.index(extChar)]
                # ========================================
                # Open the file in binary mode. Read the 
                # entire file into a pg binary object
                # ========================================
                pgBlob = None
                with open(o.path.join(imagesDirName, blobFile), 'rb') as blobFile:
                    pgBlob = pg.Binary(blobFile.read())
                # ========================================
                # Set up a list with the all parameters:
                # blob and id
                # Use the cursor to execute SQL
                # Send in the list params to be swapped
                # ========================================
                parameters = [pgBlob, parcelId]
                cur.execute(sqlWithParameters, parameters)
                del(pgBlob)
                del(parameters)
except Exception as e1:
    tb.print_exc()

