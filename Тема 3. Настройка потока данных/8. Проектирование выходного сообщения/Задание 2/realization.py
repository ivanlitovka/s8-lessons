from datetime import datetime

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as f
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, TimestampType, IntegerType


def spark_init(test_name) -> SparkSession:
    spark_jars_packages = ",".join([
        "org.postgresql:postgresql:42.4.0",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0",
    ])

    return (
        SparkSession.builder
        .master("local")
        .appName(test_name)
        .config("spark.jars.packages", spark_jars_packages)
        .config("spark.jars.repositories", "https://maven.aliyun.com/repository/public")
        .getOrCreate()
    )


postgresql_settings = {
    'user': 'student',
    'password': 'de-student'
}


def read_marketing(spark: SparkSession) -> DataFrame:
    return (
        spark.read
        .format("jdbc")
        .option("url", "jdbc:postgresql://rc1a-fswjkpli01zafgjm.mdb.yandexcloud.net:6432/de")
        .option("driver", "org.postgresql.Driver")
        .option("dbtable", "public.marketing_companies")
        .option("user", postgresql_settings['user'])
        .option("password", postgresql_settings['password'])
        .load()
    )


kafka_security_options = {
    'kafka.security.protocol': 'SASL_SSL',
    'kafka.sasl.mechanism': 'SCRAM-SHA-512',
    'kafka.sasl.jaas.config': 'org.apache.kafka.common.security.scram.ScramLoginModule required username=\"de-student\" password=\"ltcneltyn\";'
}


def read_client_stream(spark: SparkSession) -> DataFrame:
    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091")
        .options(**kafka_security_options)
        .option("subscribe", "student.topic.cohort16.ewanlitovka")
        .load()
    )

    schema = StructType([
        StructField("client_id", StringType()),
        StructField("timestamp", DoubleType()),
        StructField("lat", DoubleType()),
        StructField("lon", DoubleType()),
    ])

    return (
        raw
        .withColumn("value", f.col("value").cast(StringType()))
        .withColumn("event", f.from_json(f.col("value"), schema))
        .select(
            f.col("event.client_id").alias("client_id"),
            f.col("event.timestamp").alias("timestamp"),
            f.col("event.lat").alias("lat"),
            f.col("event.lon").alias("lon"),
        )
        .withColumn(
            "timestamp",
            f.from_unixtime(
                f.col("timestamp"),
                "yyyy-MM-dd' 'HH:mm:ss.SSS"
            ).cast(TimestampType())
        )
        .withWatermark("timestamp", "10 minutes")
        .dropDuplicates(["client_id", "timestamp"])
    )


def join(user_df, marketing_df) -> DataFrame:
    R = 6371000  # радиус Земли в метрах

    joined = user_df.crossJoin(marketing_df)

    renamed = joined.select(
        f.col("client_id").alias("client_id"),
        f.col("id").alias("adv_campaign_id"),
        f.col("name").alias("adv_campaign_name"),
        f.col("description").alias("adv_campaign_description"),
        f.col("start_time").alias("adv_campaign_start_time"),
        f.col("end_time").alias("adv_campaign_end_time"),
        f.col("point_lat").alias("adv_campaign_point_lat"),
        f.col("point_lon").alias("adv_campaign_point_lon"),
        f.current_timestamp().alias("created_at"),
        f.col("lat").alias("lat"),
        f.col("lon").alias("lon"),
    )

    # haversine
    dlat = f.col("adv_campaign_point_lat") - f.col("lat")
    dlon = f.col("adv_campaign_point_lon") - f.col("lon")

    a = (
        f.sin(f.radians(dlat) / 2) ** 2
        + f.cos(f.radians(f.col("lat")))
        * f.cos(f.radians(f.col("adv_campaign_point_lat")))
        * f.sin(f.radians(dlon) / 2) ** 2
    )
    c = 2 * f.atan2(f.sqrt(a), f.sqrt(1 - a))
    distance_m = R * c

    return (
        renamed
        .withColumn("distance", distance_m.cast(IntegerType()))
        .filter(f.col("distance") < 1000)
        .select(
            "client_id",
            "distance",
            "adv_campaign_id",
            "adv_campaign_name",
            "adv_campaign_description",
            "adv_campaign_start_time",
            "adv_campaign_end_time",
            "adv_campaign_point_lat",
            "adv_campaign_point_lon",
            "created_at",
        )
    )


if __name__ == "__main__":
    spark = spark_init('join stream')
    client_stream = read_client_stream(spark)
    marketing_df = read_marketing(spark)
    result = join(client_stream, marketing_df)

    query = (result
             .writeStream
             .outputMode("append")
             .format("console")
             .option("truncate", False)
             .start())
    query.awaitTermination()