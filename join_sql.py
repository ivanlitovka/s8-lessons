
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.types import StructType, StructField, StringType

#необходимая библиотека для интеграции Spark и PostgreSQL
spark_jars_packages = ",".join(
        [
            "org.postgresql:postgresql:42.4.0",
        ]
    )

#создаём SparkSession и передаём библиотеку для работы с PostgreSQL
spark = SparkSession.builder \
    .appName("join data") \
    .config("spark.jars.packages", spark_jars_packages) \
    .getOrCreate()

#вычитываем данные из таблицы
tableDF = spark.read \
                    .format('jdbc') \
                    .option('url', 'jdbc:postgresql://rc1a-fswjkpli01zafgjm.mdb.yandexcloud.net:6432/de') \
                    .option('driver', 'org.postgresql.Driver') \
                    .option('dbtable', 'words') \
                    .option('user', 'student') \
                    .option('password', 'de-student') \
                    .load()

#определяем схему для DataFrame
userSchema = StructType([StructField("text", StringType(), True)])

#читаем текст из файла
wordsDF = spark.readStream.schema(userSchema).format('text').load('/datas8')

#разделяем слова по запятым
splitWordsDF = wordsDF.select(explode(split(wordsDF.text, ",")).alias("word"))

#объединяем данные. Присоединяем данные из таблицы к данным из файла 
joinDF = splitWordsDF.join(tableDF, splitWordsDF.word == tableDF.words, 'left')

#проверяем каких слов нет в таблице, но есть в файле filter(...isNull())
#возвращаем только один столбец (select(...))
#убираем дубли (distinct())
filterDF = joinDF.filter(joinDF.id.isNull()).select(joinDF.word).distinct()

#запускаем стриминг
filterDF.writeStream \
    .format("console") \
    .start() \
    .awaitTermination() 


from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("SparkStreamingApplicationWindowFunction") \
    .getOrCreate()

symbolsDF = spark \
    .readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

# streaming DataFrame of schema { timestamp: Timestamp, symbol: String }

windowedCounts = symbolsDF.groupBy( \
    window(symbolsDF.timestamp, "5 minutes", "2 minutes"),\
    symbolsDF.symbol \
).count()