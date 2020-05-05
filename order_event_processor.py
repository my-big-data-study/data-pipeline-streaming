from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *


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
        .select(col('Title'), col('Quantity'), col('timestamp')) \
        .writeStream \
        .outputMode('append') \
        .format('console') \
        .option('truncate', 'false') \
        .start() \
        .awaitTermination()


if __name__ == '__main__':
    spark_session = SparkSession \
        .builder \
        .appName('Order Event Streaming Processor') \
        .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:2.4.4')\
        .config('spark.driver.host', '127.0.0.1') \
        .master('local') \
        .getOrCreate()
    run(spark_session)