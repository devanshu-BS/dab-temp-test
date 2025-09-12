# Databricks notebook source

import pandas as pd
import pygsheets as pg
import json
import numpy as np
from datetime import datetime


type = dbutils.secrets.get(scope="gheet-conn", key="type")
project_id = dbutils.secrets.get(scope="gheet-conn", key="project_id")
private_key_id = dbutils.secrets.get(scope="gheet-conn", key="private_key_id")
private_key = dbutils.secrets.get(scope="gheet-conn", key="private_key")
client_email = dbutils.secrets.get(scope="gheet-conn", key="client_email")
client_id = dbutils.secrets.get(scope="gheet-conn", key="client_id")
auth_uri = dbutils.secrets.get(scope="gheet-conn", key="auth_uri")
token_uri = dbutils.secrets.get(scope="gheet-conn", key="token_uri")
auth_provider_x509_cert_url = dbutils.secrets.get(scope="gheet-conn", key="auth_provider_x509_cert_url")
client_x509_cert_url = dbutils.secrets.get(scope="gheet-conn", key="client_x509_cert_url")

def key():
    variables_keys = {
        "type": type,
        "project_id": project_id,
        "private_key_id": private_key_id,
        "private_key": private_key,
        "client_email": client_email,
        "client_id": client_id,
        "auth_uri": auth_uri,
        "token_uri": token_uri,
        "auth_provider_x509_cert_url": auth_provider_x509_cert_url,
        "client_x509_cert_url": client_x509_cert_url
    }
    return json.dumps(variables_keys)

def read(sheetUrl, worksheetName):
    try:
        gc = pg.authorize(service_account_json=key())
        sheet = gc.open_by_url(str(sheetUrl))
        worksheet = sheet.worksheet('title', str(worksheetName))

        # Get entire data as dataframe with no header
        df_raw = worksheet.get_as_df(has_header=False)

        # Use second row as column names, skip the first row
        df_raw.columns = df_raw.iloc[1]
        df = df_raw.iloc[2:].copy()

        # Reset index
        df.reset_index(drop=True, inplace=True)

        
        # Create a dictionary to track seen columns and their first occurrence index
        seen_columns = {}
        duplicate_columns = []
        
        # Identify duplicate columns
        for i, col in enumerate(df.columns):
            if col in seen_columns:
                duplicate_columns.append((i, col))
            else:
                seen_columns[col] = i
        
        # Print duplicate columns for debugging
        # if duplicate_columns:
        #     print(f"Found {len(duplicate_columns)} duplicate columns: {[col for _, col in duplicate_columns]}")
        
        # Remove duplicate columns by keeping only the first occurrence
        df = df.loc[:, ~df.columns.duplicated()]
        
        # Print remaining columns after deduplication
        # print("Columns after deduplication:", df.columns.tolist())

        return df
    except Exception as e:
        print(repr(e))

locationsDf = read(
    'https://docs.google.com/spreadsheets/d/12LKYbYaBhnpNc8r0OZ7bu04PANO0F0tMgr-RK3QFi_M/edit?gid=647257651#gid=647257651',
    'Pivot'
)

# Clean non-prefixed columns (jarvis and others)
for col in ['clusterLat','clusterLon','distance','jarvisLat', 'jarvisLon', 'reportingBatteries', 'isVerified']:
    if col in locationsDf.columns:
        locationsDf[col] = locationsDf[col].replace(['-', '', ' '], np.nan)
        if col == 'isVerified':
            locationsDf[col] = locationsDf[col].fillna(0).astype(int)
        else:
            locationsDf[col] = pd.to_numeric(locationsDf[col], errors='coerce')

# locationsDf.display()

# Figure out the correct case of the column names
partner_id_col = None
comment_col = None
for col in locationsDf.columns:
    if col.lower() == 'partnerid':
        partner_id_col = col
    elif col.lower() == 'comment':
        comment_col = col

# # Check if we found the appropriate columns
if not partner_id_col:
    raise ValueError(f"Partner ID column not found. Available columns: {locationsDf.columns.tolist()}")

# Drop empty PartnerId rows
locationsDf = locationsDf[locationsDf[partner_id_col].notna() & (locationsDf[partner_id_col] != '')]

# Rename Cluster I columns to standard
locationsDf = locationsDf.rename(columns={
    'Cluster I_clusterLat': 'clusterLat',
    'Cluster I_clusterLon': 'clusterLon',
    'Cluster I_distance': 'distance'
})

# Convert data types
cols_to_float = ['jarvisLat', 'jarvisLon', 'clusterLat', 'clusterLon', 'distance', 'reportingBatteries']
for col in cols_to_float:
    if col in locationsDf.columns:
        locationsDf[col] = locationsDf[col].replace(['-', '', ' '], np.nan)
        locationsDf[col] = pd.to_numeric(locationsDf[col], errors='coerce')

if 'isVerified' in locationsDf.columns:
    locationsDf['isVerified'] = locationsDf['isVerified'].replace(['', '-', ' '], 0).astype(int)

# Rename columns
column_mapping = {}
if partner_id_col:
    column_mapping[partner_id_col] = 'partnerId'
if comment_col:
    column_mapping[comment_col] = 'comment'
locationsDf = locationsDf.rename(columns=column_mapping)

# Add date
locationsDf['date'] = str(datetime.now())[:10]

# Check if all final columns exist, else create them with defaults
final_columns = [
    'date', 'partnerId', 'jarvisLat', 'jarvisLon', 'status', 'reportingBatteries',
    'clusterLat', 'clusterLon', 'distance', 'isVerified', 'comment', 'locationStatus'
]

for col in final_columns:
    if col not in locationsDf.columns:
        print(f"Warning: Creating missing column '{col}' with default values")
        if col in ['jarvisLat', 'jarvisLon', 'clusterLat', 'clusterLon', 'distance', 'reportingBatteries']:
            locationsDf[col] = np.nan
        elif col == 'isVerified':
            locationsDf[col] = 0
        elif col in ['status', 'comment', 'locationStatus']:
            locationsDf[col] = ''
        # date is already added above

# Make sure we only have one instance of each column
# This is a safety check in case any duplications were created during processing
locationsDf = locationsDf.loc[:, ~locationsDf.columns.duplicated()]

# Select final desired columns that exist
existing_final_columns = [col for col in final_columns if col in locationsDf.columns]
locationsDf = locationsDf[existing_final_columns]

locationsDff = spark.createDataFrame(locationsDf)

# locationsDff.display()
locationsDff.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable("deltalake.silver.partner_location_integrity_logs")
