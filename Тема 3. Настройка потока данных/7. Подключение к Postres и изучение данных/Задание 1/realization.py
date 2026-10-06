from pyspark.sql import SparkSession

spark = (
        SparkSession.builder.appName('test_name')
        .master('local')
        .config("spark.jars.packages", "org.postgresql:postgresql:42.4.0")
        .getOrCreate()
    )

df = (spark.read
        .format("jdbc")
        .option("url", "jdbc:postgresql://rc1a-fswjkpli01zafgjm.mdb.yandexcloud.net:6432/de")
        .option("dbtable", "public.marketing_companies")
        .option("user", "student")
        .option("password", "de-student")
        .option("driver", "org.postgresql.Driver")
        .load())
df.count()