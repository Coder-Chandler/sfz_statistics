from pyspark.sql import SparkSession
from utils import log

spark = SparkSession \
    .builder \
    .appName("split_100w") \
    .config("spark.sql.parquet.compression.codec", "gzip") \
    .master("local[2]") \
    .getOrCreate()

path = "/Users/chandler/Documents/Data/NumPhone/clean_sfz/1-200/1-200_.csv"
df = spark.read.format("csv").load(path)

df.printSchema()
print(df.count())
data = df.filter("_c0<=60000")
print(data.count())

df.withColumn("col1", "$hour"+1).show()


