from pyparsing import col
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


def run(spark):
    schema = StructType([
        StructField('timestamp', TimestampType()),
        StructField('payload', StructType([
            StructField('orderId', StringType()),
            StructField('items', ArrayType(StructType([
                StructField('title', StringType()),
                StructField('quantity', IntegerType())
            ])))
        ])
                    )
    ])

    df = spark \
        .readStream \
        .format('kafka') \
        .option('kafka.bootstrap.servers', '127.0.0.1:9092') \
        .option('subscribe', 'order_event') \
        .load()

    df \
        .withColumn('value', from_json(col('value').cast('string'), schema)) \
        .select(col('value.*')) \
        .withColumn('item', explode(col('payload.items'))) \
        .withColumn('Title', col('item.title')) \
        .withColumn('Quantity', col('item.quantity')) \
        .withColumn('timestamp', to_timestamp(col('timestamp'))) \
        .select(col('Title'), col('Quantity'), col('timestamp')) \
        .withWatermark("timestamp", "1 seconds") \
        .groupBy(window('timestamp', "10 seconds", "3 seconds"), col('Title')).agg(sum('Quantity').alias('count')) \
        .orderBy(col('window.start').asc()) \
        .writeStream \
        .outputMode('complete') \
        .format('console') \
        .option('truncate', 'false') \
        .start() \
        .awaitTermination()


if __name__ == '__main__':
    spark_session = SparkSession \
        .builder \
        .appName('Order Event Streaming Processor') \
        .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.4') \
        .config('spark.driver.host', '127.0.0.1') \
        .master('local') \
        .getOrCreate()
    run(spark_session)