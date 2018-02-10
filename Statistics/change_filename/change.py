from utils import log
import os
import shutil
import csv
import xlwt
import sys
import re

csv.field_size_limit(sys.maxsize)


# 读取一个目录返回一个list
def read_path(path):
    pathlist = []
    for file in os.listdir(path):
        if file.startswith(".") or file.startswith("_"):
            continue
        file_path = os.path.join(path, file)
        pathlist.append(file_path)
    return pathlist


# 批量重命名目录下的所有文件名
def rename(path):
    root_path = read_path(path)
    for sub_one in [path for path in root_path]:
        print("sub_one路径")
        print(sub_one)
        root_path = read_path(sub_one)
        for sub_two in root_path:
            count = 1
            root_path = read_path(sub_two)
            for sub_three in root_path:
                root_path = read_path(sub_three)
                csv = root_path[0]
                print("sub_three路径")
                print(sub_three)
                print("csv路径")
                print(root_path[0])
                print(count)
                os.rename(csv, sub_three + "/{}.csv".format(str(count)))
                print("修改后的csv路径")
                print(csv)
                count += 1
                shutil.move(csv, sub_one)


# 把csv文件转换成excel文件
def csv_to_excel(path, savepath):
    with open(path, 'r', encoding='utf-8') as f:
        read = csv.reader(f)
        workbook = xlwt.Workbook()
        sheet = workbook.add_sheet('data')  # 创建一个sheet表格
        l = 0
        try:
            for line in read:
                # print(line)
                r = 0
                for i in line:
                    if len(i) > 1000:
                        for j in i:
                            sheet.write(l, r, j)  # 一个一个将单元格数据写入
                    sheet.write(l, r, i)  # 一个一个将单元格数据写入
                    r = r + 1
                l = l + 1
        except Exception:
            log(Exception)
            log("出错path ->", path)
        workbook.save(savepath)  # 保存Excel


# 把目录下的所有csv转换车excel
def csv_excel(path):
    root_path = read_path(path)
    for sub_one in [path for path in root_path]:
        # print(sub_one)
        root_path = read_path(sub_one)
        for sub_two in root_path:
            if sub_two.endswith("province"):
                continue
            # print(sub_two)
            savepath = sub_two.split(".")
            savepath[1] = "xls"
            savepath = ".".join(savepath)
            # print(savepath)
            csv_to_excel(sub_two, savepath)

def rename_100w(path):
    root_path = read_path(path)
    for sub_one in root_path:
        root_path = read_path(sub_one)
        for path in root_path:
            if path.endswith(".csv") or path.endswith(".sh"):
                continue
            os.rename(path, path+".csv")
            print(path)
            s = 'abcdefghijklmnopqrstuvwxyz'
            for i in s:
                if i in path:
                    savepath = path.split(".")
                    savepath[1] = "xls"
                    savepath = ".".join(savepath)
                    # print(savepath)
                    csv_to_excel(path, savepath)

def rename_xls(path):
    root_path = read_path(path)
    for sub_one in root_path:
        root_path = read_path(sub_one)
        for path in root_path:
            print(path)
            savepath = path.split(".")
            print(savepath)
            savepath[1] = "xls"
            savepath = ".".join(savepath)
            print(savepath)
            csv_to_excel(path, savepath)



if __name__ == "__main__":
    path = "/Users/chandler/Documents/Data/NumPhone/sales/Province"
    # rename(path)
    # csv_excel(path)
    path100w = "/Users/chandler/Documents/Data/NumPhone/clean_sfz/"
    # rename_100w(path100w)
    rename_xls(path100w)

