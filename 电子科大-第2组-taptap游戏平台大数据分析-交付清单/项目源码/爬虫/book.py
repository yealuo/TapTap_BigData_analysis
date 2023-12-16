# -*- codeing = utf-8 -*-
# @Time :2023/7/20 15:50
# @Author:X
# @File : 3.py
# @Software: PyCharm

import csv
import json

import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup
from lxml import etree


# 获取网页信息


def getData(url):
    request = requests.session()
    headers = {'user-agent':
                   'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/27.0.4281.147 '
                   'Safari/532.36 Edg/87.0.632.15'
               }
    response = request.get(url, headers=headers)
    json_list = response.json()
    listx = json_list['data']['list']
    kv = {
        '游戏名': '',
        '标签': '',
        '评分': ''
    }
    cl = []

    for index in listx:
        if type(index.get('app', '{}')) == dict :
            kv['游戏名'] = index.get('app', '{}').get('title', 'null')
            kv['评分'] = index.get('app', '{}').get('stat', '{}').get('rating', '{}').get('score', 'null')
            clist = index['app']['tags']
            for c in clist:
                cl.append(c['value'])
                kv['标签'] = cl
        else:
            a = json.loads(index.get('app', '{}'))
            kv['游戏名'] = a.get('title', 'null')
            kv['标签'] = a.get('value', 'null')
            kv['评分'] = a.get('score', 'null')
        cl = []
        write(kv)


# 提取网页信息

def write(data):
    with open('3_ios.csv', 'a', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['游戏名', '标签', '评分'])
        writer.writeheader()
        writer.writerow(data)

def main():
    url1 = 'https://www.taptap.cn/webapiv2/app-top/v2/hits?type_name=reserve&dataSource=iOS&X-UA=V%3D1%26PN' \
           '%3DWebApp%26LANG%3Dzh_CN%26VN_CODE%3D102%26VN%3D0.1.0%26LOC%3DCN%26PLT%3DPC%26DS%3DiOS%26UID' \
           '%3Da9ae6eee-b937-4488-840c-dddd2189413d%26DT%3DPC%26OS%3DWindows%26OSV%3D15.0.0'
    url2 = 'https://www.taptap.cn/webapiv2/app-top/v2/hits?dataSource=iOS&from=10&limit=10&type_name=reserve&X-UA' \
           '=V%3D1%26PN%3DWebApp%26LANG%3Dzh_CN%26VN_CODE%3D102%26VN%3D0.1.0%26LOC%3DCN%26PLT%3DPC%26DS%3DiOS' \
           '%26UID%3Da9ae6eee-b937-4488-840c-dddd2189413d%26DT%3DPC%26OS%3DWindows%26OSV%3D15.0.0'
    url3 = 'https://www.taptap.cn/webapiv2/app-top/v2/hits?dataSource=iOS&from=20&limit=10&type_name=reserve&X-UA' \
           '=V%3D1%26PN%3DWebApp%26LANG%3Dzh_CN%26VN_CODE%3D102%26VN%3D0.1.0%26LOC%3DCN%26PLT%3DPC%26DS%3DiOS' \
           '%26UID%3Da9ae6eee-b937-4488-840c-dddd2189413d%26DT%3DPC%26OS%3DWindows%26OSV%3D15.0.0'
    url4 = 'https://www.taptap.cn/webapiv2/app-top/v2/hits?dataSource=iOS&from=30&limit=10&type_name=reserve&X-UA' \
           '=V%3D1%26PN%3DWebApp%26LANG%3Dzh_CN%26VN_CODE%3D102%26VN%3D0.1.0%26LOC%3DCN%26PLT%3DPC%26DS%3DiOS' \
           '%26UID%3Da9ae6eee-b937-4488-840c-dddd2189413d%26DT%3DPC%26OS%3DWindows%26OSV%3D15.0.0'
    url5 = 'https://www.taptap.cn/webapiv2/app-top/v2/hits?dataSource=iOS&from=40&limit=10&type_name=reserve&X-UA' \
           '=V%3D1%26PN%3DWebApp%26LANG%3Dzh_CN%26VN_CODE%3D102%26VN%3D0.1.0%26LOC%3DCN%26PLT%3DPC%26DS%3DiOS' \
           '%26UID%3Da9ae6eee-b937-4488-840c-dddd2189413d%26DT%3DPC%26OS%3DWindows%26OSV%3D15.0.0'
    url6 = 'https://www.taptap.cn/webapiv2/app-top/v2/hits?dataSource=iOS&from=50&limit=10&type_name=reserve&X-UA' \
           '=V%3D1%26PN%3DWebApp%26LANG%3Dzh_CN%26VN_CODE%3D102%26VN%3D0.1.0%26LOC%3DCN%26PLT%3DPC%26DS%3DiOS' \
           '%26UID%3Da9ae6eee-b937-4488-840c-dddd2189413d%26DT%3DPC%26OS%3DWindows%26OSV%3D15.0.0'
    url7 = 'https://www.taptap.cn/webapiv2/app-top/v2/hits?dataSource=iOS&from=60&limit=10&type_name=reserve&X-UA' \
           '=V%3D1%26PN%3DWebApp%26LANG%3Dzh_CN%26VN_CODE%3D102%26VN%3D0.1.0%26LOC%3DCN%26PLT%3DPC%26DS%3DiOS' \
           '%26UID%3Da9ae6eee-b937-4488-840c-dddd2189413d%26DT%3DPC%26OS%3DWindows%26OSV%3D15.0.0'
    url8 = 'https://www.taptap.cn/webapiv2/app-top/v2/hits?dataSource=iOS&from=70&limit=10&type_name=reserve&X-UA' \
           '=V%3D1%26PN%3DWebApp%26LANG%3Dzh_CN%26VN_CODE%3D102%26VN%3D0.1.0%26LOC%3DCN%26PLT%3DPC%26DS%3DiOS' \
           '%26UID%3Da9ae6eee-b937-4488-840c-dddd2189413d%26DT%3DPC%26OS%3DWindows%26OSV%3D15.0.0'
    url9 = 'https://www.taptap.cn/webapiv2/app-top/v2/hits?dataSource=iOS&from=80&limit=10&type_name=reserve&X-UA' \
           '=V%3D1%26PN%3DWebApp%26LANG%3Dzh_CN%26VN_CODE%3D102%26VN%3D0.1.0%26LOC%3DCN%26PLT%3DPC%26DS%3DiOS' \
           '%26UID%3Da9ae6eee-b937-4488-840c-dddd2189413d%26DT%3DPC%26OS%3DWindows%26OSV%3D15.0.0'
    url10 = 'https://www.taptap.cn/webapiv2/app-top/v2/hits?dataSource=iOS&from=90&limit=10&type_name=reserve&X' \
            '-UA=V%3D1%26PN%3DWebApp%26LANG%3Dzh_CN%26VN_CODE%3D102%26VN%3D0.1.0%26LOC%3DCN%26PLT%3DPC%26DS%3DiOS' \
            '%26UID%3Da9ae6eee-b937-4488-840c-dddd2189413d%26DT%3DPC%26OS%3DWindows%26OSV%3D15.0.0'
    url11 = 'https://www.taptap.cn/webapiv2/app-top/v2/hits?dataSource=iOS&from=100&limit=10&type_name=reserve&X' \
            '-UA=V%3D1%26PN%3DWebApp%26LANG%3Dzh_CN%26VN_CODE%3D102%26VN%3D0.1.0%26LOC%3DCN%26PLT%3DPC%26DS%3DiOS' \
            '%26UID%3Da9ae6eee-b937-4488-840c-dddd2189413d%26DT%3DPC%26OS%3DWindows%26OSV%3D15.0.0'
    url12 = 'https://www.taptap.cn/webapiv2/app-top/v2/hits?dataSource=iOS&from=110&limit=10&type_name=reserve&X' \
            '-UA=V%3D1%26PN%3DWebApp%26LANG%3Dzh_CN%26VN_CODE%3D102%26VN%3D0.1.0%26LOC%3DCN%26PLT%3DPC%26DS%3DiOS' \
            '%26UID%3Da9ae6eee-b937-4488-840c-dddd2189413d%26DT%3DPC%26OS%3DWindows%26OSV%3D15.0.0'
    url13 = 'https://www.taptap.cn/webapiv2/app-top/v2/hits?dataSource=iOS&from=120&limit=10&type_name=reserve&X' \
            '-UA=V%3D1%26PN%3DWebApp%26LANG%3Dzh_CN%26VN_CODE%3D102%26VN%3D0.1.0%26LOC%3DCN%26PLT%3DPC%26DS%3DiOS' \
            '%26UID%3Da9ae6eee-b937-4488-840c-dddd2189413d%26DT%3DPC%26OS%3DWindows%26OSV%3D15.0.0'
    url14 = 'https://www.taptap.cn/webapiv2/app-top/v2/hits?dataSource=iOS&from=130&limit=10&type_name=reserve&X' \
            '-UA=V%3D1%26PN%3DWebApp%26LANG%3Dzh_CN%26VN_CODE%3D102%26VN%3D0.1.0%26LOC%3DCN%26PLT%3DPC%26DS%3DiOS' \
            '%26UID%3Da9ae6eee-b937-4488-840c-dddd2189413d%26DT%3DPC%26OS%3DWindows%26OSV%3D15.0.0'
    url15 = 'https://www.taptap.cn/webapiv2/app-top/v2/hits?dataSource=iOS&from=140&limit=10&type_name=reserve&X' \
            '-UA=V%3D1%26PN%3DWebApp%26LANG%3Dzh_CN%26VN_CODE%3D102%26VN%3D0.1.0%26LOC%3DCN%26PLT%3DPC%26DS%3DiOS' \
            '%26UID%3Da9ae6eee-b937-4488-840c-dddd2189413d%26DT%3DPC%26OS%3DWindows%26OSV%3D15.0.0'
    url_list = []
    url_list.append(url1)
    url_list.append(url2)
    url_list.append(url3)
    url_list.append(url4)
    url_list.append(url5)
    url_list.append(url6)
    url_list.append(url7)
    url_list.append(url8)
    url_list.append(url9)
    url_list.append(url10)
    url_list.append(url11)
    url_list.append(url12)
    url_list.append(url13)
    url_list.append(url14)
    url_list.append(url15)
    for i in url_list:
        getData(i)

if __name__=='__main__':
    main()

