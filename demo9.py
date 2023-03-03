#coding=utf-8

import requests
import json
import pymysql
import re
import math
import schedule
import time
import pandas as pd
from bs4 import BeautifulSoup



class macaujob123:
    def __init__(self):

        self.cookie = ''
    
    def sql_login(self):
        self.db = pymysql.connect(host='127.0.0.1',port=3306,user='dl_oyjob_cn',passwd='FKH5RasXC3mtbCwm',database='dl_oyjob_cn',charset='utf8mb4')
        self.cursor = pymysql.cursors.DictCursor(self.db)
        

    def sql(self):
        self.db = pymysql.connect(host='127.0.0.1',port=3306,user='dl_oyjob_cn',passwd='FKH5RasXC3mtbCwm',database='dl_oyjob_cn',charset='utf8mb4')
        self.cursor = pymysql.cursors.DictCursor(self.db)
        query = "SELECT * FROM `dev_source_cate`"
        self.cursor.execute(query)
        self.rows = self.cursor.fetchall()

        query1 = "SELECT * FROM `dev_category`"
        self.cursor.execute(query1)
        self.rows1 = self.cursor.fetchall()

        query2 = "SELECT * FROM `dev_task_item`"
        self.cursor.execute(query2)
        rows2 = self.cursor.fetchall()
        self.item_nos = [x['item_no'] for x in rows2]
        

    def login(self):
        
        url = 'https://www.macaujob123.com/victorbsds/api/login.php?act=login'
        headers = {
            'accept':'text/html, */*; q=0.01',
            'accept-encoding':'gzip, deflate, br',
            'accept-language':'zh-CN,zh;q=0.9',
            'cache-control':'no-cache',
            'content-length':'38',
            'content-type':'application/x-www-form-urlencoded; charset=UTF-8',
            'origin':'https://www.macaujob123.com',
            'pragma':'no-cache',
            'referer':'https://www.macaujob123.com/victorbsds/login.php',
            'sec-ch-ua':'"Not_A Brand";v="99", "Google Chrome";v="109", "Chromium";v="109"',
            'sec-ch-ua-mobile':'?0',
            'sec-ch-ua-platform':'"Windows"',
            'sec-fetch-dest':'empty',
            'sec-fetch-mode':'cors',
            'sec-fetch-site':'same-origin',
            'user-agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36',
            'x-requested-with':'XMLHttpRequest',
                    }
        formdata = 'username=18024168676&password=karl1011'
        req = requests.post(url,headers = headers,data = formdata)
        cookies = req.cookies.items()
        cookie = ''
        for name, value in cookies:
            cookie += '{0}={1};'.format(name, value)
        self.cookie = cookie

    def job_index(self,page):
        url = 'https://www.macaujob123.com/victorbsds/view/job/index.php'
        headers = {
            'accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
            'accept-encoding':'gzip, deflate, br',
            'accept-language':'zh-CN,zh;q=0.9',
            'cache-control':'no-cache',
            'cookie':self.cookie,
            'pragma':'no-cache',
            'referer':'https://www.macaujob123.com/victorbsds/index.php',
            'sec-ch-ua':'"Not_A Brand";v="99", "Google Chrome";v="109", "Chromium";v="109"',
            'sec-ch-ua-mobile':'?0',
            'sec-ch-ua-platform':'"Windows"',
            'sec-fetch-dest':'iframe',
            'sec-fetch-mode':'navigate',
            'sec-fetch-site':'same-origin',
            'upgrade-insecure-requests':'1',
            'user-agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36',
        }
       
        params = 'myshow=1&status=1&bid=0&language=0&cid=0&hot=-1&isurgent=-1&gender=0&age=0&paixu=id&xu=desc&comefrom_id=-1&pagesize=30&keywords=&isinner=-2&isfake=0&fadan=0&showdownload=0&sdate=0&formdate=0&page={}'.format(page)
        try:
            req = requests.get(url,headers = headers,timeout=10,params=params)
            pd.read_html(req.text)[0]
            return req
        except BaseException as e:
            print(e)
            return self.job_index(page)
        
    
    def parse(self,req):
        df = pd.read_html(req.text)[0]
        sourcedic = {'没有字母的': '领城',
            'OY': '澳业',
            'JY': '集英',
            'GM': '专注',
            'GMB': '专注',
            'GMA': '专注',
            'BL': '伯乐',
            'LCLW': '力创',
            'ZC': '卓诚',
            'D': '友嘉',
            'WS': '唯曙',
            'Four': '四季',
            'G': '国霖',
            'D0020': '大把人',
            'JA': '家澳',
            'JX': '捷讯',
            'XC': '新诚',
            'HZ': '汇致',
            'HX': '汇贤',
            'OK': '澳启',
            'AJ': '澳基',
            'AZ': '澳智',
            'OL': '澳洛',
            'OA': '澳赞',
            'RF': '睿僼',
            'Sx': '胜浠'}
        query = "SELECT * FROM `dev_source_cate`"
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        querydic = {}
        for k in rows:
            querydic.update({k['title']:k['id']})

        for row in df.itertuples():
            
            res = {}
            ID = {'ID' : getattr(row,'ID')}
            res.update({'Order_number': getattr(row,'单号')})
            res.update({'item_no':getattr(row,'单号')})
            res.update({'title' : getattr(row,'职位名称')})
            price = getattr(row,'薪资').split('~')
            if price[0] == '面谈' or price[0] == '面议':
                res.update({'min_price' : 0})
            else:
                try:
                    res.update({'min_price' : int(price[0])})
                    res.update({'max_price' : int(price[1])})
                except:
                    res.update({'min_price' : int(price[0])})
                    res.update({'max_price' : 0})

                
            res.update({'sign_stop_time' : getattr(row,'报名').replace('/','-').replace('时',':').replace('分','')})#报名截至时间

            Order_number = str(getattr(row,'单号')).split(re.findall('\d+',str(getattr(row,'单号')))[0])[0]

            if Order_number == '':
                sourceid = 17
                res['sourctxt'] = '领城'
            else: 
                Order = Order_number.split('-')[0].split('_')[0].strip()
                if Order in sourcedic.keys():
                    source = sourcedic[Order]
                    
                    sourceid = querydic[source]
                    res['sourctxt'] = source
             


            rest = self.get_tabel2(ID['ID'])
            try:
                res['source'] = sourceid
                
                res.update(rest)

                res['create_time'] = res['sign_up_time'].split(' ')[0]
                if res['item_no'] not in self.item_nos:
                    create_time = res['create_time']
                    
                    if int(time.mktime(time.strptime('2023-01-01','%Y-%m-%d'))) <= int(time.mktime(time.strptime(create_time,'%Y-%m-%d'))):
                        if self.bool == True:
                            if '全部' in self.modify_price.keys():
                                you_price = self.modify_price['全部'] + int(res['work_price'])
                                res['you_price'] = you_price
                                print(res)
                                self.updata_res(res)
                            elif res['sourctxt'] in self.modify_price.keys():
                                you_price = self.modify_price[res['sourctxt']] + int(res['work_price'])
                                res['you_price'] = you_price
                                print(res)
                                self.updata_res(res)

                        elif self.sourcebool == True:
                            if rest['sourctxt'] not in self.sourcelist:
                                self.updata_res(res)
                                print(res)
                        else:
                            self.updata_res(res)
                            print(res)
                else:
                    # print('数据已存在')

                    return 1

                
            except BaseException as e:
                print(e)
                print('单号',getattr(row,'单号'))
                # with open('ttt.txt','w')as f:
                #     f.write(str(getattr(row,'单号')))

        


    def updata_res(self,res):
        url = 'https://dl.oyjob.cn/api/index/addTaskItem'
        req = requests.post(url,data = res)
        print(req.text)
        # with open('tt.txt','w')as f:
        #     f.write(str(req.text))

    def insert_table(self,temp,data):
        try:
            rowdic = {

            }
            for row in self.rows1:
                rowdic.update({row['title']:row['id']})
                
            if temp['职位类别'] in rowdic.keys():
                typeid = rowdic[temp['职位类别']]
                return typeid
            else:
                #执行数据库插入操作
                print('.........',data)
                self.cursor.execute("insert into `dev_category` (title) values('%s')" %(data))
                #提交
                self.db.commit()
                rowdic = {

                }
                self.sql()
                for row in self.rows1:
                    rowdic.update({row['title']:row['id']})
                typeid = rowdic[temp['职位类别']]
                return typeid
        except BaseException as e:
            print('insert_table',e)
            self.sql()
            return self.insert_table(temp,data)

    def get_tabel2(self,ID):
        url = 'https://www.macaujob123.com/victorbsds/view/job/view.php?id={}&backuri=index.php%3Fmyshow%3D1%26status%3D1%26bid%3D0%26language%3D0%26cid%3D0%26hot%3D-1%26isurgent%3D-1%26gender%3D0%26age%3D0%26paixu%3Did%26xu%3Ddesc%26comefrom_id%3D-1%26pagesize%3D30%26keywords%3D%26page%3D1%26isinner%3D-2%26isfake%3D0%26fadan%3D0%26showdownload%3D0%26sdate%3D0%26formdate%3D'.format(ID)
        headers = {
            'accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
            'accept-encoding':'gzip, deflate, br',
            'accept-language':'zh-CN,zh;q=0.9',
            'cache-control':'no-cache',
            'cookie':self.cookie,
            'pragma':'no-cache',
            'referer':'https://www.macaujob123.com/victorbsds/view/job/index.php?myshow=1&status=1&bid=0&language=0&cid=0&hot=-1&isurgent=-1&gender=0&age=0&paixu=id&xu=desc&comefrom_id=-1&pagesize=30&keywords=&page=1&isinner=-2&isfake=0&fadan=0&showdownload=0&sdate=0&formdate=0',
            'sec-ch-ua':'"Not_A Brand";v="99", "Google Chrome";v="109", "Chromium";v="109"',
            'sec-ch-ua-mobile':'?0',
            'sec-ch-ua-platform':'"Windows"',
            'sec-fetch-dest':'iframe',
            'sec-fetch-mode':'navigate',
            'sec-fetch-site':'same-origin',
            'sec-fetch-user':'?1',
            'upgrade-insecure-requests':'1',
            'user-agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36',
        }
        
        req = requests.get(url,headers = headers)
        temp = {}
        for df in pd.read_html(req.text):
            for row in df.itertuples(index = False):
                temp.update({row[0]:row[1]})
        # work = temp['工作时间'].split('~')
        sex_age = temp['性别/年龄'].split(' / ')
        if sex_age[0] == '男':
            sex = 2
        elif sex_age[0] == '女':
            sex = 3
        else:
            sex = 1
            
        if temp['面试方式'] == '现场':
            face_type = 1
        else:
            face_type = 2
    
        typeid = self.insert_table(temp,temp['职位类别'])
        if temp['面试时间'] == '待定':
            face_time = 0
        else:
            face_time = temp['面试时间']
        soup = BeautifulSoup(req.text,'lxml')
        yaoqiu = ''
        for tr in soup.find('table').find_all('tr'):
            if '任职要求' in str(tr.find_all('td')):
                for p in tr.find_all('td')[1].find_all('p'):
                    yaoqiu += p.text + '\n'
        rest = {
            'zp_renshu':int(re.findall('\d+',temp['招聘人数'])[0]),
            'xueli':temp['学历要求'],
            'sign_up_time':temp['发布时间'],
    #         'start_work':work[0],
            'end_work':temp['工作时间'],
            'eat_zhu': temp['吃住情况'],
            'jiaqi': temp['假期安排'],
            'yaoqiu': yaoqiu,
            'work_id': '',  #职位ID 不要
            'create_user':  17,  #职位ID 不要
            'face_address' : temp['面试地点'],  #要修改名称
            
            'face_time':face_time, 
            'work_price':int(temp['职位原单价']), 
            'you_price' : 0,#根据条件修改
            'type' : typeid,
    #         'type' : 183,
            'is_jipin' : '',   #急聘 0-否 1-是
            'language' : temp['语言要求'],
            
            'sex':sex,
            'age':int(re.findall('\d+',sex_age[1])[0]),
            'other_give':temp['其他待遇'],
            'face_value' : temp['面试结果'],
            'remark':temp['特殊备注'],
            'jiaobiao_time' : ' '.join(temp['交表时间'].split(' ')[1:]).replace('/','-').replace('时',':').replace('分',''),
    #         'face_type' : temp['面试方式'],
            'face_type' : face_type,
            'admin_id' : 1,
            'update_time':'',
            'delete_time':'',
            'face_stop_time':'',
            'title':temp['职位名称'].split(' ')[0],
        }
        return rest
    

    def start_request(self):
        self.sql_login()
        self.sql()
        self.login()
        req = self.job_index(1)
        res = self.parse(req)
        if res != False:
            soup = BeautifulSoup(req.text,'lxml')
            pt = int(soup.find('span',class_="pt").text.split('：')[1])
            pages = math.ceil(int(pt)/30)
            for page in range(2,pages+1):
                req = self.job_index(page)
                res = self.parse(req)
                if res == False:
                    break


    def main(self):
        years = time.strftime('%Y-%m-%d',time.localtime(time.time()))
        start_time_str = '08:00:00'
        timestamp  = time.strptime(years + ' '+start_time_str,'%Y-%m-%d %H:%M:%S')
        start_time = int(time.mktime(timestamp))
        end_time_str = '23:59:59'
        timestamp  = time.strptime(years + ' '+end_time_str,'%Y-%m-%d %H:%M:%S')
        end_time = int(time.mktime(timestamp))

        #设置要修改的价格，示例：  { '来源1':100,     设置来源的时候就不要设置全部
        #                           '来源2':200}
        self.modify_price = {
            '全部': 500,
        }

        self.bool = True    # 如果为True,则修改指定职位价格，False 不对价格进行修改

        self.sourcelist = []     #  示例   [ '集英','来源2']  中括号里面的引号要用英文的
        self.sourcebool = False  # 如果为True,就指定哪个来源不爬

        self.start_request()
#        schedule.every(60).minutes.do(self.start_request)
#        while True:

#            if int(time.time()) > start_time and int(time.time()) < end_time:
#                schedule.run_pending()

if __name__ == '__main__':
    macaujob123().main()
