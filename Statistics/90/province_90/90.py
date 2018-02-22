from pyspark.sql import SparkSession
from utils import log

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

spark = SparkSession \
    .builder \
    .appName("province") \
    .config("spark.sql.parquet.compression.codec", "gzip") \
    .master("local[2]") \
    .getOrCreate()


def dataframe(filename):
    path = "/Users/chandler/Documents/Data/NumPhone/clean_sfz/" + str(filename) + "/" + str(filename) + ".csv"
    df = spark.read.format("csv").load(path)
    return df


def savedata(filename, province):
    total = 0
    for j in province:
        count = 0
        log("处理" + str(province[j]) + "省数据")
        for i in filename:
            log("处理" + str(i) + "万数据 ->", i)
            df = dataframe(filename[i])
            df.createOrReplaceTempView("people")
            search = "%{}%".format(str(province[j]))
            sql = "select * from people where _c8 like '{}' and _c7 > 19850101 and _c6 = 'F' ".format(
                search)
            data = spark.sql(str(sql))
            log(str(i) + "有" + str(data.count()) + "条是{}的数据 ->".format(province[j]), data.count())
            count += data.count()
            data.show(5)
            data.coalesce(1).write.format("csv").save(
                "/Users/chandler/Documents/Data/NumPhone/90/province_90/{}/".format(j) + str(i),
                mode="overwrite")
        total += count
        log("{}共有数据数量 ->".format(province[j]), count)
        log("==============================={}执行完毕======================================\n\n".format(province[j]))
    log("===================>总数据量 ->", total)


filename = {"1-200": "1-200", "200-400": "200-400", "400-600": "400-600",
            "600-800": "600-800", "800-1000": "800-1000", "1000-1200": "1000-1200",
            "1200-1400": "1200-1400", "1600-1800": "1600-1800", "1800-2000": "1800-2000"}
province = {"shandong": "山东", "jiangsu": "江苏", "anhui": "安徽", "shangahi": "上海", "zhejiang": "浙江", "tianjin": "天津",
            "beijing": "北京", "hebei": "河北", "liaoning": "辽宁", "jilin": "吉林", "heilongjiang": "黑龙江", "fujian": "福建",
            "guangdong": "广东", "guangxi": "广西", "yunnan": "云南", "hainan": "海南", "aomen": "澳门", "xianggang": "香港",
            "taiwan": "台湾", "jiangxi": "江西", "hunan": "湖南", "guizhou": "贵州", "hubei": "湖北", "henan": "河南",
            "shanxi1": "山西", "shanxi3": "陕西", "chongqing": "重庆", "sichuan": "四川", "qinghai": "青海", "gansu": "甘肃",
            "ningxia": "宁夏", "neimeng": "内蒙", "xinjiang": "新疆", "xizang": "西藏", }
if __name__ == "__main__":
    savedata(filename, province)
