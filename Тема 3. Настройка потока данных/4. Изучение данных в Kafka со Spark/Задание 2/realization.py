from pyspark.sql import SparkSession
from pyspark.sql import functions as f, DataFrame
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType

# необходимая библиотека с идентификатором в maven
# вы можете использовать ее с помощью метода .config и опции "spark.jars.packages"
kafka_lib_id = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0"

# настройки security для кафки
# вы можете использовать из с помощью метода .options(**kafka_security_options)
kafka_security_options = {
    'kafka.security.protocol': 'SASL_SSL',
    'kafka.sasl.mechanism': 'SCRAM-SHA-512',
    'kafka.sasl.jaas.config': 'org.apache.kafka.common.security.scram.ScramLoginModule required username=\"de-student\" password=\"ltcneltyn\";',
}


def spark_init() -> SparkSession:
    """Создаёт SparkSession с подключённой библиотекой Spark ↔ Kafka."""
    return (
        SparkSession.builder
        .master("local")
        .appName("test connection to kafka")
        .config("spark.jars.packages", kafka_lib_id)
        .getOrCreate()
    )


def load_df(spark: SparkSession) -> DataFrame:
    """Читает топик persist_topic из Kafka как статичный DataFrame."""
    return (
        spark.read
        .format("kafka")
        .option("kafka.bootstrap.servers", "rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091")
        .options(**kafka_security_options)
        .option("subscribe", "persist_topic")
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .load()
    )


def transform(df: DataFrame) -> DataFrame:
    """Декодирует key/value, парсит JSON и разворачивает его в колонки."""
    subscription_schema = StructType([
        StructField("subscription_id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("description", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("currency", StringType(), True),
    ])

    return (
        df
        .select(
            f.col("key").cast("string").alias("key"),
            f.col("value").cast("string").alias("value"),
            f.col("topic"),
            f.col("partition"),
            f.col("offset"),
            f.col("timestamp"),
            f.col("timestampType"),
        )
        .withColumn("parsed", f.from_json(f.col("value"), subscription_schema))
        .select(
            f.col("parsed.subscription_id").alias("subscription_id"),
            f.col("parsed.name").alias("name"),
            f.col("parsed.description").alias("description"),
            f.col("parsed.price").alias("price"),
            f.col("parsed.currency").alias("currency"),
            f.col("key"),
            f.col("value"),
            f.col("topic"),
            f.col("partition"),
            f.col("offset"),
            f.col("timestamp"),
            f.col("timestampType"),
        )
    )


spark = spark_init()

source_df = load_df(spark)
df = transform(source_df)


df.printSchema()
df.show(truncate=False)


'''
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType
)

spark_jars_packages = 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0'

spark = SparkSession.builder \
    .master('local') \
    .appName('test connection to kafka') \
    .config('spark.jars.packages', spark_jars_packages) \
    .getOrCreate()

persistDF = spark.read \
    .format('kafka') \
    .option('kafka.bootstrap.servers', 'rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091') \
    .option('kafka.security.protocol', 'SASL_SSL') \
    .option('kafka.sasl.mechanism', 'SCRAM-SHA-512') \
    .option(
        'kafka.sasl.jaas.config',
        'org.apache.kafka.common.security.scram.ScramLoginModule required '
        'username="de-student" password="ltcneltyn";'
    ) \
    .option('subscribe', 'persist_topic') \
    .option('startingOffsets', 'earliest') \
    .option('endingOffsets', 'latest') \
    .load()

subscription_schema = StructType([
    StructField('subscription_id', IntegerType(), True),
    StructField('name', StringType(), True),
    StructField('description', StringType(), True),
    StructField('price', DoubleType(), True),
    StructField('currency', StringType(), True),
])

resultDF = persistDF \
    .select(
        col('key').cast('string').alias('key'),
        col('value').cast('string').alias('value'),
        col('topic'),
        col('partition'),
        col('offset'),
        col('timestamp'),
        col('timestampType')
    ) \
    .withColumn('parsed', from_json(col('value'), subscription_schema)) \
    .select(
        col("parsed.subscription_id").alias("subscription_id"),
        col("parsed.name").alias("name"),
        col("parsed.description").alias("description"),
        col("parsed.price").alias("price"),
        col("parsed.currency").alias("currency"),
        col("key"),
        col("value"),
        col("topic"),
        col("partition"),
        col("offset"),
        col("timestamp"),
        col("timestampType"),
    )

resultDF.printSchema()
resultDF.show(truncate=False)
'''