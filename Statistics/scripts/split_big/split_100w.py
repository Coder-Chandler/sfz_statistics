from pyspark.sql import SparkSession
from utils import log

spark = SparkSession \
    .builder \
    .appName("split_100w") \
    .config("spark.sql.parquet.compression.codec", "gzip") \
    .master("local[2]") \
    .getOrCreate()

path = "/Users/chandler/Documents/Data/NumPhone/clean_sfz/1-200/1-200.csv"
df = spark.read.format("csv").load(path)

df.printSchema()
log("原文件大小 ->", df.count())
print(df.count())
data = df.filter("_c0<=60000")
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
                    "/Users/chandler/Documents/Data/NumPhone/sales/100W/{}".format(j), mode="overwrite")


filename = {"1-6W": 60000, "6W-12W": 120000, "12W-18W": 180000, "24W-30W": 300000, "36W-42W": 420000, "48W-54W": 540000,
            "60W-66W": 660000, "72W-78W": 780000, "84W-90W": 900000, "96W-102W": 1020000, }

if __name__ == "__main__":
    original_size = df.count()
    modify_size = 60000
    csv_split(original_size, modify_size)
