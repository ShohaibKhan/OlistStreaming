from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

REGION="ap-south-1"
STREAM_NAME="STREAMNAME"

RAW_S3 ="s3://S3BUCKET/raw/events"
PROCESSED_S3="s3://S3BUCKET/processed/"

CHECKPOINT_RAW = "s3://S3BUCKET/processed/checkpoints/raw/"
CHECKPOINT_ORDERS = "s3://S3BUCKET/processed/checkpoints/orders/"
CHECKPOINT_ITEMS = "s3://S3BUCKET/processed/checkpoints/items/"
CHECKPOINT_PAYMENTS = "s3://S3BUCKET/processed/checkpoints/payments/"

spark = SparkSession.builder.appName("kinesis-streaming").getOrCreate()

raw = spark.readStream.format("aws-kinesis") \
   .option("kinesis.region", REGION) \
   .option("kinesis.streamName", STREAM_NAME) \
   .option("kinesis.consumerType", "GetRecords") \
   .option("kinesis.endpointUrl", "KINESIS_URL") \
   .option("kinesis.startingposition", "TRIM_HORIZON") \
   .load()


#raw = spark.readStream.format("kinesis").option("streamName",STREAM_NAME).option("region",REGION).option("initialPosition","TRIM_HORIZON").load()
json_df = raw.selectExpr("CAST(data as STRING) as json_str")

q_raw = json_df.writeStream.format("json").option("path",RAW_S3).option("checkpointLocation",CHECKPOINT_RAW).outputMode("append").start()


schema = StructType([
    StructField("event_type", StringType()),
    StructField("event_time", StringType()),
    StructField("order_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("product_id", StringType()),
    StructField("seller_id", StringType()),
    StructField("order_status", StringType()),
    StructField("data", MapType(StringType(), StringType()))
])




parsed = json_df.select(from_json(col("json_str"),schema).alias("e")).select("e.*")

orders = parsed.filter(col("event_type")=="ORDER_SNAPSHOT")
items = parsed.filter(col("event_type")=="ITEM_ADDED")
payments = parsed.filter(col("event_type")=="PAYMENT_RECEIVED")


# handling orders

orders_clean = orders.select(
                                col("order_id"),
                                col("customer_id"),
                                lower(trim(col("order_status"))).alias("order_status"),
                                to_timestamp(col("data")["order_purchase_timestamp"]).alias("purchase_ts"),
                                to_timestamp(col("data")["order_approved_at"]).alias("approved_ts"),
                                to_timestamp(col("data")["order_delivered_carrier_at"]).alias("carrier_ts"),
                                to_timestamp(col("data")["order_delivered_customer_date"]).alias("delivered_ts"),
                                to_timestamp(col("data")["order_estimated_delivery_date"]).alias("estimated_ts"),
                                current_timestamp().alias("ingested_at")

                                ).filter(col("order_id").isNotNull())

orders_clean = orders_clean.withColumn("purchase_date",to_date(col("purchase_ts")))
orders_clean = orders_clean.withColumn("delivery_days",datediff(col("delivered_ts"),col("purchase_ts")))
orders_clean = orders_clean.withColumn("delivery_delay_days",datediff(col("delivered_ts"),col("estimated_ts")))

orders_clean = orders_clean.filter(col("purchase_ts").isNotNull())
orders_clean = orders_clean.dropDuplicates(["order_id"])


# handling items

items_clean = items.select(
    col("order_id"),
    col("product_id"),
    col("seller_id"),
    col("data")["order_item_id"].cast("int").alias("order_item_id"),
    to_timestamp(col("data")["shipping_limit_date"]).alias("shipping_limit_ts"),
    col("data")["price"].cast("double").alias("price"),
    col("data")["freight_value"].cast("double").alias("freight_value"),  # Fixed
    current_timestamp().alias("ingested_at")
).filter(col("order_id").isNotNull()).filter(col("order_item_id").isNotNull()).filter(col("price").isNotNull())

items_clean = items_clean.dropDuplicates(["order_id","order_item_id"])



# handling payments


payments_clean = payments.select(
                            
                        col("order_id"),
                        col("data")["payment_sequential"].cast("int").alias("payment_sequential"),
                        lower(trim(col("data")["payment_type"])).alias("payment_type"),
                        col("data")["payment_installments"].cast("int").alias("payment_installments"),
                        col("data")["payment_value"].cast("double").alias("payment_value"),
                        current_timestamp().alias("ingested_at")
            
        ).filter(col("order_id").isNotNull()).filter(col("payment_value")>0)

payments_clean = payments_clean.dropDuplicates(["order_id","payment_sequential"])




q_orders = orders_clean.writeStream.format("parquet").option("path",PROCESSED_S3+"orders/").option("checkpointLocation",CHECKPOINT_ORDERS).trigger(processingTime="5 seconds").outputMode("append").partitionBy("purchase_date").start()

q_items = items_clean.writeStream.format("parquet").option("path",PROCESSED_S3+"items/tems/").option("checkpointLocation",CHECKPOINT_ITEMS).trigger(processingTime="5 seconds").outputMode("append").start()

q_payments = payments_clean.writeStream.format("parquet").option("path",PROCESSED_S3+"payments/payments").option("checkpointLocation",CHECKPOINT_PAYMENTS).trigger(processingTime="5 seconds").outputMode("append").start()


spark.streams.awaitAnyTermination()



