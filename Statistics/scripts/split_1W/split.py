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
    print(range_count)
    start_size = 1
    step_size = modify_size
    for i in range(range_count):
        print(i)
        data = df.filter("_c0>={} and _c0<={}".format(start_size, modify_size))
        data.show()
        start_size = modify_size
        modify_size += step_size
        for j in filename:
            if filename[j] == start_size:
                data.coalesce(1).write.format("csv").save(
                    "/Users/chandler/Documents/Data/NumPhone/sales/1W/{}".format(j), mode="overwrite")


filename = {"1-1W": 10000, "1W-2W": 20000, "2W-3W": 30000, "3W-4W": 40000, "4W-5W": 50000, "5W-6W": 60000,
            "6W-7W": 70000, "7W-8W": 80000, "8W-9W": 90000, "9W-10W": 100000, }

if __name__ == "__main__":
    original_size = df.count()
    modify_size = 10000
    csv_split(original_size, modify_size)
