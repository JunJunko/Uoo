__author__ = 'Junko'

# -*- coding: utf-8 -*-
import urllib2
import urllib
import re


localCSV = r'..//shares_data//'


def getSharesList():
    url = r'http://quote.eastmoney.com/stocklist.html'
    request = urllib2.Request(url)
    response = urllib2.urlopen(request).read()

    r = r'<a target="_blank" href=.*?>.*?\((.*?)\)</a>'
    code = re.findall(r, response)
    for i in code:
        if re.match(r'^0', i) or re.match(r'^6', i):
            # print i
            getCsv(str(i))
    # return code

def getWebData(code):
    url = r'http://quotes.money.163.com/trade/lsjysj_'+code+'.html'
    request = urllib2.Request(url)
    response = urllib2.urlopen(request).read()
    return response


def getMinDate(code):
    patter = '<input type=\"radio\" name=\"date_start_type\" value=\"(.*?)\" checked=\"checked\">'
    mindate = re.search(patter, getWebData(code))
    return mindate.group(1).replace('-','')

def getMaxDate(code):
    patter = '<input type=\"text\" name=\"date_end_value\" style=\"width:120px\" value=\"(.*?)\">'
    maxdate = re.search(patter, getWebData(code))
    return maxdate.group(1).replace('-','')



def getCsv(code):
    if code[0:1] == '0':
        url = r'http://quotes.money.163.com/service/chddata.html?code='+'1'+code+'&start='+getMinDate(code)+'&end='+getMaxDate(code)+'&fields=TCLOSE;HIGH;LOW;TOPEN;LCLOSE;CHG;PCHG;TURNOVER;VOTURNOVER;VATURNOVER;TCAP;MCAP'
    else:
        url = r'http://quotes.money.163.com/service/chddata.html?code='+'0'+code+'&start='+getMinDate(code)+'&end='+getMaxDate(code)+'&fields=TCLOSE;HIGH;LOW;TOPEN;LCLOSE;CHG;PCHG;TURNOVER;VOTURNOVER;VATURNOVER;TCAP;MCAP'
    data = urllib.urlopen(url).read().replace('None','0')
    with open(localCSV+'\\'+code+'.csv', 'w') as f:
        f.write(data)
    return  url


if __name__ == '__main__':
    # print getCsv(str("000150"))
    print getSharesList()