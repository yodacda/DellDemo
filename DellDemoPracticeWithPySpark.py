import findspark
findspark.init()
import psycopg2
from pyspark.sql.functions import *
from pyspark.sql.functions import count
from pyspark.sql import SparkSession
from urllib.request import urlopen

#creating the SparkSession
spark = SparkSession.builder.appName("test").master("local").getOrCreate()
#Reading the data from WebAPI and storing in PySpark Data Frame.
url = "https://opendata.ecdc.europa.eu/covid19/nationalcasedeath/json/"
jsonData = urlopen(url).read().decode('utf-8')
rdd = spark.sparkContext.parallelize([jsonData])
df = spark.read.json(rdd)
#df.printSchema()
#print(df.count())
#df.show()

#Reading the existing data from Postgres DB and storing it in PySpark DataFrame
covidDF=spark.read.format("jdbc").option("url","jdbc:postgresql://localhost:5432/delldemo")\
    .option("dbtable","covid19final").option("user","postgres").option("password","pgpassword")\
    .option("driver","org.postgresql.Driver").load()
#print(covidDF.count())


#exceptAll will give us back only not matching data from first data frame which is df here.
updatedCovidData = df.exceptAll(covidDF)
#print(updatedCovidData.count())
#Collect except data from 1st data frame and read it.
collect = updatedCovidData.collect()
j=0
for i in collect:
     j += 1
#if data is more than one row, then store in Postgres DB otherwise it will print no update.
if j > 0:
    updatedCovidData.write.mode("overwrite") \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/delldemo")\
        .option("dbtable", "covid19final") \
        .option("user", "postgres") \
        .option("password", "pgpassword") \
        .option("driver", "org.postgresql.Driver") \
        .save()
else:
    print("there are no updates")
