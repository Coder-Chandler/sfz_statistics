from pyspark.sql import SparkSession
from utils import log

spark = SparkSession \
    .builder \
    .appName("split") \
    .config("spark.sql.parquet.compression.codec", "gzip") \
    .master("local[2]") \
    .getOrCreate()

path = "/Users/chandler/Documents/Data/NumPhone/sales/10W/10W_.csv"
df = spark.read.format("csv").load(path)

df.printSchema()
log("原文件大小 ->", df.count())
print(df.count())
data = df.filter("_c0<=10000")
log("过滤的文件大小 ->", data.count())
log("========================================================\n")
print(data.count())


def csv_split(original_size, modify_size):
    range_count = int(original_size / modify_size)
    start_size = 0
    step_size = modify_size
    for i in range(range_count):
        data = df.filter("_c0>={} and _c0<={}".format(start_size, modify_size))
        start_size = modify_size
        modify_size += step_size
        data.coalesce(1).write.format("csv").save(
            "/Users/chandler/Documents/Data/NumPhone/sales/1W/", mode="overwrite")


if __name__ == "__main__":
    original_size = df.count()
    modify_size = 10000
    csv_split(original_size, modify_size)
