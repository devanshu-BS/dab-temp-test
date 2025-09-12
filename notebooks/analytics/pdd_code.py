# Databricks notebook source


import os, sys
sys.path.append('../../')
sys.path.append('../../src/utils')
from queryHelper import *
from databaseHelper import *
import pandas as pd
import pygsheets as pg
import os
import json
from pyspark.sql.functions import current_timestamp, expr, from_utc_timestamp, current_date, datediff, col, lit, when, to_timestamp
from pyspark.sql.types import *
from datetime import datetime, timedelta



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

def read(sheetUrl,worksheetName):
    try:
        gc = pg.authorize(service_account_json=key())
        sheet = gc.open_by_url(str(sheetUrl))
        worksheet = sheet.worksheet('title',str(worksheetName))
        return worksheet.get_as_df()
    except Exception as e: print(repr(e))

def write(sheetUrl,worksheetName,data, clear = True):
    try:
        gc = pg.authorize(service_account_json=key())
        sheet = gc.open_by_url(str(sheetUrl))
        worksheet = sheet.worksheet('title',str(worksheetName))
        if (clear == True):
            worksheet.clear()
        worksheet.set_dataframe(data,(1,1),False)
        return print("Written successfully.")
    except Exception as e: print(repr(e))

df_batt = spark.sql("""
    select id as batteryId, serialNo,iotDeviceNo, batteryType, manufacturerName, occupant, isMisplaced
    from deltalake.bronze.batteries 
    where deletedAt is null
""")

batt_last_charging = spark.sql("""
    select 
        deviceId as iotDeviceNo, 
        max(endTs) as last_charging_ts 
    from deltalake.silver.charging_discharging_events where state = 'charging'
    group by deviceId
""")

batt_last_txn = spark.sql("""
    select 
       serialNo,max(createdAt) as lastTxnTs
    from deltalake.silver.battery_last_transaction group by serialNo
""")
# from pyspark.sql.functions import col, datediff, current_date

# # Get distinct device IDs as a list
# deviceIdList = [row['iotDeviceNo'] for row in df_batt.select("iotDeviceNo").distinct().collect()]

# # Convert the list to a string for SQL IN clause
# deviceIdStr = ','.join([f"'{id}'" for id in deviceIdList])
# batt_last_valid_ping = spark.sql(f'''
#     SELECT deviceID as iotDeviceNo,MAX(start_time) AS last_valid_ping_ts 
#     FROM deltalake.bronze.iot_resampled 
#     WHERE voltage_first > 0 AND deviceID IN ({deviceIdStr}) 
#     GROUP BY deviceID
# ''')





# Define the start and end time for the last 7 days
start_time = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d %H:%M:%S')
end_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

# Get the start and end day
start_day = int((datetime.now() - timedelta(days=7)).strftime('%d'))
end_day = int(datetime.now().strftime('%d'))

# Define the day condition
day_condition = "day <= 31" if start_day > end_day else f"day between {min(start_day, end_day)} and {max(start_day, end_day)}"

query = f"""
WITH base_iot AS (
    SELECT 
        deviceID AS iotDeviceNo, 
        start_time AS ts, 
        soc_first as soc, 
        voltage_first AS voltage, 
        current_first AS current, 
        first_lat AS lat,
        first_lon AS lon,
        LAG(first_lat) OVER (PARTITION BY deviceID ORDER BY start_time) AS prevLat, 
        LAG(first_lon) OVER (PARTITION BY deviceID ORDER BY start_time) AS prevLon, 
        LAG(start_time) OVER (PARTITION BY deviceID ORDER BY start_time) AS prevTs
    FROM 
        deltalake.bronze.iot_resampled i
    WHERE 
        year in (year(timestamp('{start_time}')), 
        year(timestamp('{end_time}')))
        AND month in (month(timestamp('{start_time}')), month(timestamp('{end_time}')))
        AND {day_condition}
        AND start_time >= timestamp '{start_time}'
        AND start_time <= timestamp '{end_time}'
        AND soc_first > 0
),
filter_base_iot AS (
    SELECT 
        *, 
        LEAD(soc) OVER (PARTITION BY iotDeviceNo ORDER BY ts) AS nextSoc 
    FROM (
        SELECT 
            *,  
            distance / timeDiff AS speed
        FROM (
            SELECT 
                *, 
                UNIX_TIMESTAMP(ts) - UNIX_TIMESTAMP(prevTs) AS timeDiff,
                2 * 637100.88 * ASIN(SQRT(
                    POWER(SIN(RADIANS(lat - prevLat) / 2), 2) +
                    COS(RADIANS(prevLat)) * COS(RADIANS(lat)) *
                    POWER(SIN(RADIANS(lon - prevLon) / 2), 2)
                )) AS distance
            FROM 
                base_iot
            WHERE 
                prevTs IS NOT NULL
        ) 
        WHERE 
            timeDiff > 0
    ) 
    WHERE 
        speed <= 10 and distance > 100
)
SELECT iotDeviceNo, max(ts) as last_movement_ts
FROM filter_base_iot 
GROUP BY iotDeviceNo
"""

batt_last_movement = spark.sql(query)

# Perform the join operation
df_batt = df_batt.join(batt_last_charging, 
                      on = 'iotDeviceNo', 
                      how = 'left')
df_batt = df_batt.join(batt_last_txn, 
                       on = 'serialNo',
                       how = 'left')
    

df_batt = df_batt.join(batt_last_movement, 
                       on = 'iotDeviceNo', 
                       how='left')

df_batt = df_batt.withColumn("days_since_last_charge", datediff(current_date(), col("last_charging_ts")))
df_batt = df_batt.withColumn("days_since_last_txn", datediff(current_date(), col("lastTxnTs")))
df_batt = df_batt.withColumn("days_since_last_movement", datediff(current_date(), col("last_movement_ts")))


# Days to DD
from datetime import datetime, timedelta

# Calculate the date two days ago
two_days_ago = datetime.now() - timedelta(days=2)
day = two_days_ago.day
month = two_days_ago.month
year = two_days_ago.year

batt_iot = spark.sql(f"""
SELECT 
    deviceID, 
    MAX(start_time) AS lastValidBmsTs, 
    MAX(voltage_first) AS voltage_first 
FROM deltalake.bronze.iot_resampled 
WHERE voltage_first > 0 
AND year >= {year} AND month >= {month} AND day >= {day}
GROUP BY deviceID
""")
df_batt = df_batt.join(
    batt_iot,
    df_batt['iotDeviceNo'] == batt_iot['deviceId'],
    how='left'
)

# Create a DataFrame from the dictionary for joining
batt_data = [
    ("NMC", "LIVGUARD", 35, 1),
    ("NMC", "STEFEN ELECTRIC", 30, 1),
    ("NMC", "INVERTED", 30, 1),
    ("LFP", "TRONTEK", 32, 1)
]

threshold_df = spark.createDataFrame(batt_data, ["batteryType", "manufacturerName", "dd_voltage", "drop_per_day"])

# Assuming you have a DataFrame 'df' with columns: batteryType, manufacturerName, voltage
# Replace 'df' with your actual DataFrame name

# Join with df_batt to add dd_voltage and drop_per_day columns
df_batt = df_batt.join(threshold_df, on=["batteryType", "manufacturerName"], how="left")

# Convert dd_voltage and drop_per_day to float where possible, else null
df_batt = df_batt.withColumn("dd_voltage", col("dd_voltage").cast("float")) \
                     .withColumn("drop_per_day", col("drop_per_day").cast("float"))

# Calculate daysTillDD
df_batt = df_batt.withColumn(
    "daysTillDD",
    when(
        (col("voltage_first").isNotNull()) & (col("dd_voltage").isNotNull()) & (col("drop_per_day").isNotNull()) & (col("drop_per_day") != 0),
        (col("voltage_first") - col("dd_voltage")) / col("drop_per_day")
    ).otherwise(None)
)

df_batt = df_batt.drop("dd_voltage", "drop_per_day")


df_batt = df_batt.withColumn(
    "daysTillDD",
    when(col("daysTillDD") < 0, 0).otherwise(col("daysTillDD"))
)
df_batt = df_batt.withColumn('lastValidBmsTs', to_timestamp(col('lastValidBmsTs')))
df_batt = df_batt.withColumn('bmsValid', 
                             expr("CASE WHEN current_timestamp() - lastValidBmsTs < INTERVAL 2 DAYS THEN 1 ELSE 0 END"))

df_batt = df_batt.withColumn(
    "priority",
    when(
        (col("bmsValid") == 1) & 
        (col("days_since_last_charge") > 7) & 
        (col("days_since_last_txn") > 7) & 
        (col("daysTillDD") < 14) & 
        (col("voltage_first") < 50), "P0"
    ).otherwise(
        when(
            (col("bmsValid") == 1) & 
            (col("days_since_last_charge") > 7) & 
            (col("days_since_last_txn") > 7) & 
            (col("daysTillDD") > 14) & 
            (col("voltage_first") < 50), "P1"
        ).otherwise(
            when(
                (col("bmsValid") == 0) & 
                (col("daysTillDD") < 14), "P2"
            ).otherwise(None)
        )
    )
)

## **Alamrs**
import pandas as pd
import json
alarms = databricksDevFetch("select * from deltalake.silver.iot_events_latest")
alarms = adbFetch("select * from iotEventsLatest ")
alarms['cellLowVoltAlarm'] = alarms['alarms'].apply(
    lambda x: json.loads(x).get('Acuvl2', False) if pd.notna(x) else False
)
alarms['totalLowVoltAlarm'] = alarms['alarms'].apply(
    lambda x: json.loads(x).get('Atvll2', False) if pd.notna(x) else False
)
alarms = alarms[['deviceID','cellLowVoltAlarm','totalLowVoltAlarm']]
alarms_spark = spark.createDataFrame(alarms)
alarms_spark = alarms_spark.withColumnRenamed("deviceID", "iotDeviceNo")
df_batt = df_batt.join(
    alarms_spark,
    "iotDeviceNo",
    how='left'
)

# Data Push
schema = StructType([
    StructField("runDate", DateType(), True),
    StructField("createdAt", TimestampType(), True),
    StructField("serialNo", StringType(), True),
    StructField("iotDeviceNo", StringType(), True),
    StructField("batteryId", StringType(), True),
    StructField("occupant", StringType(), True),
    StructField("batteryType", StringType(), True),
    StructField("manufacturerName", StringType(), True),
    StructField("isMisplaced", BooleanType(), True),
    StructField("last_charging_ts", TimestampType(), True),
    StructField("lastTxnTs", TimestampType(), True),
    StructField("deviceID", StringType(), True),
    StructField("lastValidBmsTs", TimestampType(), True),
    StructField("voltage_first", FloatType(), True),
    StructField("daysTillDD", DoubleType(), True),
    StructField("bmsValid", IntegerType(), True),
    StructField("priority", StringType(), True),
    StructField("cellLowVoltAlarm", BooleanType(), True),
    StructField("totalLowVoltAlarm", BooleanType(), True)
])

batt_data = spark.sql('''
SELECT
  l.batteryId,
  DATEDIFF(CURRENT_DATE(), DATE(MAX(l.updatedAt))) AS max_date_diff
FROM deltalake.bronze.battery_logs l
INNER JOIN deltalake.bronze.batteries b
  ON l.batteryId = b.id
WHERE l.isMisplaced = false
  AND l.changedBy NOT LIKE '%USCI%'
  AND l.occupant NOT LIKE '%WH%'
  AND b.deletedAt IS NULL
  AND l.createdAt < current_timestamp() - INTERVAL 16 HOURS
GROUP BY l.batteryId
''')
df_batt = df_batt.join(batt_data, on="batteryId", how="left")
volt_pd_df = prodFetch('''select distinct b.serialNo from scannedAssetInMovements sam
left join tasks t on t.id= sam.taskId
left join batteries b on b.id=sam.assetId
where t.customerId like 'WH%' and sam.assetType ='battery' and sam.assetHandlingType='drop' and b.status='service'
and b.occupant like 'WH%' 
and sam.createdAt >= now()-interval 1 day''')
volt_spark_df = spark.createDataFrame(volt_pd_df)

# Collect serial numbers from volt_spark_df
serialNos = [row.serialNo for row in volt_spark_df.select("serialNo").collect()]

pdd_batteries = df_batt.filter(
    (
  
        (col("bmsValid") == 1) &
        (col("voltage_first").between(30, 52)) &
        (col("days_since_last_txn") > 7)
    ) |
    (
        (col("max_date_diff") > 14) &
        (expr("lastValidBmsTs IS NULL OR lastValidBmsTs < current_timestamp() - INTERVAL 7 DAYS"))
    ) |
    (
        col("serialNo").isin(serialNos)
    )
).withColumn(
    "flag",
    when(
        (col("bmsValid") == 1) &
        (col("voltage_first").between(30, 52)) &
        (col("days_since_last_txn") > 7),
        lit("IoT")
    ).when(
        (col("max_date_diff") > 14) &
        (expr("lastValidBmsTs IS NULL OR lastValidBmsTs < current_timestamp() - INTERVAL 7 DAYS")),
        lit("Non IoT")
    ).when(
        col("serialNo").isin(serialNos),
        lit("Voltage Reconable")
    ).otherwise(None)
)

# batteries = spark.sql("SELECT serialNo, status FROM deltalake.bronze.batteries")

# pdd_batteries = pdd_batteries.join(
#     batteries,
#     on="serialNo",
#     how="left"
# ).filter(
#     batteries.status != "service"
# ).drop("status")

# display(pdd_batteries)

pdd_batteries_pd = pdd_batteries.toPandas()
pushDf(pdd_batteries_pd, "pdd_batteries","replace")
df_batt = df_batt.withColumn("runDate", current_date())
df_batt = df_batt.withColumn("createdAt", from_utc_timestamp(current_timestamp(), "Asia/Kolkata"))

# COMMAND ----------

df_batt.write.format("delta").option("schema", schema).option("mergeSchema", "true").mode("overwrite").saveAsTable("deltalake.silver.pdd_latest")

# COMMAND ----------

df_batt.write.format("delta").option("mergeSchema", "true").mode("append").saveAsTable("deltalake.silver.pdd_logs")
