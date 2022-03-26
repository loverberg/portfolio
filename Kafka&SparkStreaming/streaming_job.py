from pyspark.sql import SparkSession

import os

from pyspark.sql import functions as f
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import collect_set, to_json, struct
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType, DoubleType

# поля справочника
dim_columns = ['id', 'name']

payment_rows = [
    (1, 'Credit card'),
    (2, 'Cash'),
    (3, 'No charge'),
    (4, 'Dispute'),
    (5, 'Unknown'),
    (6, 'Voided trip'),
]

trips_schema = StructType([
    StructField('vendor_id', StringType(), True),
    StructField('tpep_pickup_datetime', TimestampType(), True),
    StructField('tpep_dropoff_datetime', TimestampType(), True),
    StructField('passenger_count', IntegerType(), True),
    StructField('trip_distance', DoubleType(), True),
    StructField('ratecode_id', IntegerType(), True),
    StructField('store_and_fwd_flag', StringType(), True),
    StructField('pulocation_id', IntegerType(), True),
    StructField('dolocation_id', IntegerType(), True),
    StructField('payment_type', IntegerType(), True),
    StructField('fare_amount', DoubleType(), True),
    StructField('extra', DoubleType(), True),
    StructField('mta_tax', DoubleType(), True),
    StructField('tip_amount', DoubleType(), True),
    StructField('tolls_amount', DoubleType(), True),
    StructField('improvement_surcharge', DoubleType(), True),
    StructField('total_amount', DoubleType(), True),
    StructField('congestion_surcharge', DoubleType()),
])


def create_dict(spark: SparkSession, header: list[str], data: list):
    df = spark.createDataFrame(data=data, schema=header)
    return df


def foreach_batch_function(df: DataFrame, epoch_id):
    df.write.mode("append").json("output_report")


def main(spark: SparkSession):
    jsonOptions = {"timestampFormat": "yyyy-MM-dd'T'HH:mm:ss.sss'Z'"}

    fields = list(map(lambda x: f"json_message.{x.name}", trips_schema.fields))

    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:29092") \
        .option("subscribe", "taxi") \
        .option("startingOffsets", "latest") \
        .option("maxOffsetsPerTrigger", 1000) \
        .load() \
        .select(f.from_json(f.col("value").cast("string"), trips_schema, jsonOptions).alias("json_message")) \
        .select(fields)

    # считаем витрину
    mart = df.groupBy('payment_type', f.weekofyear('tpep_pickup_datetime').alias('week')).agg(
        f.avg(f.col('total_amount')).alias('avg_total_amount'),
        (f.sum(f.col('tip_amount')) / f.sum(f.col('total_amount'))).alias('tip_ratio')) \
        .join(
        other=create_dict(spark, dim_columns, payment_rows),
        on=f.col('payment_type') == f.col('id'),
        how='inner') \
        .select(f.col('week'), f.col('name'), f.col('avg_total_amount'), f.col('tip_ratio')) \
        .orderBy(f.col('name'), f.col('week')) \
        .select(to_json(struct('week', 'name', 'avg_total_amount', 'tip_ratio')).alias('value'))

    # использую для отладки
    # writer = mart \
    #     .writeStream \
    #     .trigger(processingTime='10 seconds') \
    #     .format("console") \
    #     .outputMode("complete") \
    #     .option("truncate", "false") \
    #     .start()

    # публикую в kafka
    writer = mart \
        .writeStream \
        .trigger(processingTime='10 seconds') \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:29092") \
        .option("topic", "report") \
        .option("checkpointLocation", "streaming-job-checkpoint") \
        .outputMode("complete") \
        .start()


    writer.awaitTermination()


if __name__ == '__main__':
    main(SparkSession.
         builder
         .appName("streaming_job")
         .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0")
         .getOrCreate())
