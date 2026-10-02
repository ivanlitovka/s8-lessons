from pyspark.sql import SparkSession

# Укажите библиотеку Kafka для Spark.
spark_jars_packages = ""

# Создайте SparkSession и передайте библиотеку через spark.jars.packages.
spark = (
    SparkSession.builder
    .master("local")
    .appName("test connect to kafka")
    .config("spark.jars.packages", spark_jars_packages)
    .getOrCreate()
)

# Подключитесь к persist_topic, используя настройки из предыдущего урока про kcat.
df = (
    spark.read
    .format("kafka")
    .option("kafka.bootstrap.servers", "")
    .option("kafka.security.protocol", "")
    .option("kafka.sasl.mechanism", "")
    .option("kafka.sasl.jaas.config", "")
    .option("subscribe", "persist_topic")
    .load()
)
