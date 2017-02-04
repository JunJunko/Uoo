__author__ = 'Junko'
 # -*- coding: utf-8 -*-

import pachong
import MySQLdb
import json
import config
import sys
reload(sys)
sys.setdefaultencoding( "utf-8" )

def csvAnalytical(code):
    file_object = open(pachong.localCSV+'\\'+code+'.csv', 'r')
    value=[]
    # file = open('D:\\tmp\\123.txt', 'r',coding='utf-8')
    line = file_object.readlines()
    for i in line:
        value.append(tuple(i.split(',')))
    file_object.close()
    del value[0]
    return value

def getDbInfo():
    data = open(r'..//py_config//mysql.json', 'r').read()
    value = json.loads(data)
    return value

def inserData(code):
    db = MySQLdb.connect(host=getDbInfo()["MYSQL"][0]["host"],user=getDbInfo()["MYSQL"][0]["user"],passwd=getDbInfo()["MYSQL"][0]["passwd"],db=getDbInfo()["MYSQL"][0]["db"])
    con = db.cursor()

    sql = r'insert into getdata (data_dt,shares_code,shares_name,end_price,hight,low,begin_prince,qsp,zde,zdf,hsl,cjl,cjje,tot_price,mark_price) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
    # print csvAnalytical(code)
    con.executemany(sql, csvAnalytical(code))
    db.commit()
    con.close()
    db.close()


if __name__ == '__main__':
    print inserData(str('000150'))
