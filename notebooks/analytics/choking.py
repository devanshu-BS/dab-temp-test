# Databricks notebook source


from datetime import datetime as dt, timedelta
import pytz
from pyspark.sql.functions import col, expr, hour, from_json, explode, max, min, when, avg, count, date_add, lit, date_format, to_timestamp, to_date
from pyspark.sql.types import ArrayType, StringType
from pyspark.sql.window import Window

# COMMAND ----------
dateStart = (dt.now()-timedelta(days=2)).replace(hour=18,minute=30,second=0).strftime("%Y-%m-%d %H:%M:%S")
dateEnd = (dt.now()-timedelta(days=1)).replace(hour=18,minute=30,second=0).strftime("%Y-%m-%d %H:%M:%S")

l3ddateStart = (dt.now()-timedelta(days=2)).replace(hour=22,minute=0,second=0).strftime("%Y-%m-%d %H:%M:%S")
l3ddateEnd = (dt.now()).replace(hour=2,minute=0,second=0).strftime("%Y-%m-%d %H:%M:%S")

# COMMAND ----------
txn_df = (
    spark.table("deltalake.bronze.transactions")
    .filter(
        (col("deletedAt").isNull())
        & (col("createdAt") >= dateStart)
        & (col("createdAt") < dateEnd)
    )
    .select(
        col("id").alias("txnId"),
        "batteriesIssued",
        "batteriesReceived",
        "driverId",
        "partnerId",
        "createdAt",
        "zoneId",
    )
)

# COMMAND ----------
txn_df = txn_df.withColumn(
    "createdAt", col("createdAt") + expr("INTERVAL 330 MINUTES")). \
        withColumn("hour", hour(col("createdAt"))). \
            withColumn("batteriesIssued", from_json(col("batteriesIssued"), ArrayType(StringType()))). \
                withColumn("batteriesReceived", from_json(col("batteriesReceived"), ArrayType(StringType()))) \
 
# COMMAND ----------
txn_issued = txn_df.withColumn("batteriesIssued", explode("batteriesIssued")).drop(
    "batteriesReceived"
)

txn_recieved = txn_df.withColumn(
    "batteriesReceived", explode("batteriesReceived")
).drop("batteriesIssued")

# COMMAND ----------
batteries = (
    spark.table("deltalake.bronze.batteries")
    .filter(col("deletedAt").isNull())
    .select("id", "iotDeviceNo")
)

# COMMAND ----------
txn_I = txn_issued.join(batteries, txn_issued.batteriesIssued == batteries.id, how="inner").drop("id")
txn_R = txn_recieved.join(batteries, txn_recieved.batteriesReceived == batteries.id, how="inner").drop("id")

iot_l3d = (
    spark.table("deltalake.bronze.iot")
    .filter(col("ts").between(l3ddateStart, l3ddateEnd))
    .select("deviceID", "ts", "soc", "voltage")
)

# COMMAND ----------
joined_I = txn_I.alias("txn").join(
    iot_l3d.alias("iot"),
    (
        (col("iot.deviceID") == col("txn.iotDeviceNo")) &
        (col("iot.voltage") > 0) &
        (col("iot.ts").between(col("txn.createdAt"), 
                               col("txn.createdAt") + expr("INTERVAL 2 HOURS")))
    ),
    how="inner"
).drop("iotDeviceNo")

# COMMAND ----------
# Define window specification
window_spec = Window.partitionBy("txnId")

# Add maxSoc and minSoc columns
joined_I = joined_I.withColumn("maxSoc", max(col("soc")).over(window_spec))
joined_I = joined_I.withColumn("minSoc", min(col("soc")).over(window_spec))

# Add maxSocTime and minSocTime columns
joined_I = joined_I.withColumn("maxSocTime", max(when(col("soc") == col("maxSoc"), col("ts"))).over(window_spec))
joined_I = joined_I.withColumn("minSocTime", min(when(col("soc") == col("minSoc"), col("ts"))).over(window_spec))

# Group by txnId and select the required columns
aggregated_I = joined_I.groupBy("txnId").agg(
    max(col("maxSoc")).alias("maxSoc"),
    min(col("minSoc")).alias("minSoc"),
    max(col("maxSocTime")).alias("maxSocTime"),
    min(col("minSocTime")).alias("minSocTime"),
    max(col("createdAt")).alias("createdAt")
)

# COMMAND ----------
# Compute idling end

#updated idling END condition on 20 August 2025
result_I = aggregated_I.withColumn(
    "idlingEnd", col("createdAt"))


# result_I = aggregated_I.withColumn(
#     "idlingEnd",
#     when(
#         (col("maxSoc") - col("minSoc") > 5) & (col("maxSocTime") < col("minSocTime")),
#         col("maxSocTime")
#     )
#     .when(col("maxSoc") - col("minSoc") > 5, col("createdAt"))
#     .otherwise(None)
# )

# COMMAND ----------
joined_R = txn_R.alias("txn").join(
    iot_l3d.alias("iot"),
    (
        (col("iot.deviceID") == col("txn.iotDeviceNo")) &
        (col("iot.voltage") > 0) &
        (col("iot.ts").between(col("txn.createdAt") - expr("INTERVAL 2 HOURS"), 
                               col("txn.createdAt")))
    ),
    how="inner"
).drop("iotDeviceNo")

# COMMAND ----------
# Add maxSoc and minSoc columns
joined_R = joined_R.withColumn("maxSoc", max(col("soc")).over(window_spec))
joined_R = joined_R.withColumn("minSoc", min(col("soc")).over(window_spec))

# Add maxSocTime and minSocTime columns
joined_R = joined_R.withColumn("maxSocTime", max(when(col("soc") == col("maxSoc"), col("ts"))).over(window_spec))
joined_R = joined_R.withColumn("minSocTime", min(when(col("soc") == col("minSoc"), col("ts"))).over(window_spec))

# Group by txnId and select the required columns
aggregated_R = joined_R.groupBy("txnId").agg(
    max(col("maxSoc")).alias("maxSoc"),
    min(col("minSoc")).alias("minSoc"),
    max(col("maxSocTime")).alias("maxSocTime"),
    min(col("minSocTime")).alias("minSocTime"),
    min(col("createdAt")).alias("createdAt")
)


# COMMAND ----------
# Compute idling start
result_R = aggregated_R.withColumn(
    "idlingStart",
    when(
        (col("maxSoc") - col("minSoc") > 5) & (col("maxSocTime") < col("minSocTime")),
        col("minSocTime")
    )
    .when((col("maxSoc") - col("minSoc") > 5), col("createdAt"))
    .otherwise(None)
)

# COMMAND ----------
final_R = (
    result_I.join(result_R, "txnId", "inner")
    .select(
        "txnId",
        col("idlingStart").cast("timestamp"),
        col("idlingEnd").cast("timestamp"),
    )
    .dropna()
    .filter(col("idlingStart") < col("idlingEnd"))
)

# COMMAND ----------
choking_df = final_R.withColumn(
    "chokingDuration", 
    (col("idlingEnd") - col("idlingStart")).cast("long")
)

transaction_waiting_time = txn_df.select(
    "txnId",
    "driverId",
    "partnerId",
    col("createdAt").cast("timestamp"),
    "zoneId",
    "hour",
).join(choking_df, ["txnId"])

# COMMAND ----------
transaction_waiting_time.withColumn("txnId", col("txnId").cast("long")).write.format(
    "delta"
).mode("append").option("mergeSchema", "true").saveAsTable(
    "deltalake.silver.transaction_waiting_time"
)

# COMMAND ----------
groupedDf = transaction_waiting_time.groupBy("partnerId", "zoneId", "hour").agg(
    avg("chokingDuration").alias("chokingDuration"),
    count("chokingDuration").alias("events")
)

# COMMAND ----------
groupedDf = groupedDf.withColumn("chokingDurationMinutes", (col("chokingDuration")/60).cast("long")). \
    withColumn("wAvg", col("chokingDurationMinutes") * col("events"))

# COMMAND ----------
groupedDf = groupedDf.withColumn(
    "date", to_date(to_timestamp(lit(dateStart)) + expr("INTERVAL 330 MINUTES"))
).withColumn("day", date_format("date", "EEEE"))

# COMMAND ----------
groupedDf.write.format("delta").mode("append").option(
    "mergeSchema", "true"
).saveAsTable("deltalake.silver.daily_choking")

# COMMAND ----------
zoneWiseIdling = transaction_waiting_time.groupBy("zoneId", "hour").agg(
    avg("chokingDuration").alias("chokingDuration"),
    count("chokingDuration").alias("events"),
)

# COMMAND ----------
zoneWiseIdling = zoneWiseIdling.withColumn(
    "chokingDurationMinutes", (col("chokingDuration") / 60).cast("long")
)

zoneWiseIdling = zoneWiseIdling.withColumn(
    "date", to_date(to_timestamp(lit(dateStart)) + expr("INTERVAL 330 MINUTES"))
).withColumn("day", date_format("date", "EEEE"))

# COMMAND ----------
zoneWiseIdling.write.format("delta").mode("append").option(
    "mergeSchema", "true"
).saveAsTable("deltalake.silver.daily_zonewise_choking")
