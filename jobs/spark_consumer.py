from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

KAFKA_BROKERS = "kafka-broker-1:19092,kafka-broker-2:19092,kafka-broker-3:19092"
SOURCE_TOPIC = "financial_transactions"
AGGREGATES_TOPIC = "aggregated_transactions"
ANOMALIES_TOPIC = "anomalies_transactions"
SPARK_CHECKPOINT_DIR = "/mnt/spark-checkpoints"
SPARK_STATE_DIR = "/mnt/spark-state"

spark = (
    SparkSession.builder
        .appName("KafkaConsumerFinancialTransactions")
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
        .config("spark.sql.streaming.checkpointLocation", SPARK_CHECKPOINT_DIR)
        .config("spark.sql.streaming.stateStore.stateStoreDir", SPARK_STATE_DIR)
        .config("spark.sql.shuffle.partitions", "40")
        .config("spark.sql.streaming.stateStore.stateSchemaCheck", "false")
        .config("spark.sql.streaming.schemaInference", "true")
        .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

transaction_schema = StructType([
    StructField("id_transacao", StringType(), True),
    StructField("id_usuario", StringType(), True),
    StructField("valor", DoubleType(), True),
    StructField("hora_transacao", LongType(), True),
    StructField("id_vendedor", StringType(), True),
    StructField("tipo_transacao", StringType(), True),
    StructField("localizacao", StringType(), True),
    StructField("metodo_pagamento", StringType(), True),
    StructField("compra_internacional", StringType(), True),
    StructField("moeda", StringType(), True)
])

kafka_stream = (
    spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKERS)
        .option("subscribe", SOURCE_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", 5000)
        .load()
)

transaction_df = (
    kafka_stream
        .selectExpr("CAST(value AS STRING)")
        .select(from_json(col("value"), transaction_schema).alias("data_transaction"))
        .select("data_transaction.*")
)

transaction_df = transaction_df.withColumn(
    "hora_transacao_ts", (col('hora_transacao') / 1000).cast("timestamp")
)

aggregated_df = (
    transaction_df.groupBy("id_vendedor")
    .agg(
        sum("valor").alias("valor_total_por_vendedor"),
        count("id_transacao").alias("numero_transacoes")
    )
)

aggregated_query = (
    aggregated_df.withColumn("key", col("id_vendedor").cast("string"))
    .withColumn(
        "value",
        to_json(
            struct(
                col("id_vendedor"),
                col("valor_total_por_vendedor"),
                col("numero_transacoes")
            )
        ),
    )
    .selectExpr("key", "value")
    .writeStream.format("kafka")
    .outputMode("update")
    .option("kafka.bootstrap.servers", KAFKA_BROKERS)
    .option("topic", AGGREGATES_TOPIC)
    .option("checkpointLocation", f"{SPARK_CHECKPOINT_DIR}/aggregates")
    .option("maxOffsetsPerTrigger", 5000)
    .trigger(processingTime="5 seconds")
)

aggregated_query = aggregated_query.start().awaitTermination()