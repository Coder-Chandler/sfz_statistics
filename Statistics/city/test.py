from pyspark.sql import SparkSession
from utils import log
from utils import province_and_city

spark = SparkSession \
    .builder \
    .appName("province") \
    .config("spark.sql.parquet.compression.codec", "gzip") \
    .master("local[2]") \
    .getOrCreate()

province_and_city = province_and_city()


def dataframe(province, filename):
    path = "/Users/chandler/Documents/Data/NumPhone/Province/{0}/province/{1}/*.csv".format(province, filename)
    df = spark.read.format("csv").load(path)
    return df


def savedata(filename, province):
    total = 0
    for j in province:
        log("处理" + str(j) + "省数据")
        for i in filename:
            log("处理" + str(i) + "万数据 ->", i)
            df = dataframe(j, filename[i])
            df.createOrReplaceTempView("people")
            for k in province_and_city:
                if province[j] in k:
                    for v in province_and_city[k]:
                        count = 0
                        search = "%{}%".format(str(v))
                        sql = "select * from people where _c8 like '{}' and _c2<99999999999 ".format(search)
                        data = spark.sql(str(sql))
                        log(str(i) + "有" + str(data.count()) + "条是{}市的数据 ->".format(str(v)), data.count())
                        count += data.count()
                        data.show(5)
                        data.coalesce(1).write.format("csv").save(
                            "/Users/chandler/Documents/Data/NumPhone/Province/{0}/city/{1}/{2}".format(j, str(v),
                                                                                                       str(i)),
                            mode="overwrite")
                    total += count
                    log("{}市共有数据数量 ->".format(province[j]), count)
                    log("==============================={}执行完毕======================================\n\n".format(
                        province[j]))
        log("===================>总数据量 ->", total)


province = {"shandong": "山东", "jiangsu": "江苏", "anhui": "安徽", "zhejiang": "浙江", "hebei": "河北", "liaoning": "辽宁",
            "province": "吉林", "heilongjiang": "黑龙江", "fujian": "福建",
            "guangdong": "广东", "guangxi": "广西", "yunnan": "云南", "hainan": "海南", "jiangxi": "江西", "hunan": "湖南",
            "guizhou": "贵州", "hubei": "湖北", "henan": "河南",
            "shanxi1": "山西", "shanxi3": "陕西", "sichuan": "四川", "qinghai": "青海", "gansu": "甘肃",
            }
filename = {"1-200": "1-200", "200-400": "200-400", "400-600": "400-600",
            "600-800": "600-800", "800-1000": "800-1000", "1000-1200": "1000-1200",
            "1200-1400": "1200-1400", "1600-1800": "1600-1800", "1800-2000": "1800-2000"}
if __name__ == "__main__":
    savedata(filename, province)
