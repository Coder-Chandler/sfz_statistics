from pyspark.sql import SparkSession
from utils import log

spark = SparkSession \
    .builder \
    .appName("90stat") \
    .config("spark.sql.parquet.compression.codec", "gzip") \
    .master("local[2]") \
    .getOrCreate()


def dataframe(filename):
    path = "/Users/chandler/Documents/Data/NumPhone/2000W/" + str(filename) + ".csv"
    df = spark.read.format("csv").load(path)
    return df


"""
val Name = "_c0"
    val CtfId = "_c4"
    val Mobile = "_c19"
    val Tel = "_c20"
    val EMail = "_c22"
    val Fax = "_c21"
    val Gender = "_c5"
    val Birthday = "_c6"
    val Address = "_c7"
    val Zip = "_c8"
"""


def savedata(filename):
    for i in filename:
        log("处理" + str(i) + "万数据 ->", i)
        df = dataframe(filename[i])
        df.createOrReplaceTempView("people")
        print(df.count())
        sql = "select _c0,_c4,_c19,_c20,_c22,_c21,_c5,_c6,_c7,_c8 from people " \
              "where _c6 > 19850101 and _c5 = 'F' and _c19!='' and _c19<99999999999"
        data = spark.sql(str(sql))
        data.show(5)
        print(data.count())
        data.coalesce(1).write.format("csv").save(
            "/Users/chandler/Documents/Data/NumPhone/90/all_90/{}".format(i), mode="overwrite")


filename = {"1-200W": "1-200W", "200W-400W": "200W-400W", "400W-600W": "400W-600W",
            "600W-800W": "600W-800W", "800W-1000W": "800W-1000W", "1000W-1200W": "1000W-1200W",
            "1200W-1400W": "1200W-1400W", "1600W-1800W": "1600W-1800W", "1800W-2000W": "1800W-2000W"}
if __name__ == "__main__":
    savedata(filename)
