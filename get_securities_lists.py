# author: alvin.constantine@outlook.com, datetime: 2021/4/17 12:35pm, License: GNU 3.0 License
# amended: 2021/10/17 20:00pm
from dateutil.relativedelta import relativedelta
from urllib.parse import urlparse
from bs4 import BeautifulSoup
from datetime import datetime
from io import StringIO
import pandas as pd
import requests
import sys
import re

class security_crawler:
    def __init__(self):
        self.twse_headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:84.0) Gecko/20100101 Firefox/84.0',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'zh-TW,en-US;q=0.8,en;q=0.5,zh;q=0.3',
            'Content-Type': 'application/x-www-form-urlencoded',
            'Origin': 'https://www.twse.com.tw',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Referer': 'https://www.twse.com.tw/',
            'Upgrade-Insecure-Requests': '1' }
        self.tpex_headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:85.0) Gecko/20100101 Firefox/85.0',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'zh-TW,en-US;q=0.8,en;q=0.5,zh;q=0.3',
            'Referer': 'https://www.tpex.org.tw/web/stock/aftertrading/otc_quotes_no1430/stk_wn1430.php?l=zh-tw',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Cache-Control': 'max-age=0' }
        self.todaytime = datetime.today()
        self.tradedaytoday = self._get_trade_day_today()

        # 上市股票每日收盤行情
        url, progress = 'https://www.twse.com.tw/exchangeReport/MI_INDEX?response=html&date=%s&type=ALLBUT0999' % self.tradedaytoday.strftime('%Y%m%d'), '上市股票每日收盤行情'
        self.df = self._get_tseallbut0999(url, progress)
        if self.df is None:
            print('在處理 %s 的 %s 時沒有資料。' % (url, progress))
            return
        print('已完成 %s 讀取及作業。' % progress)

        # 上櫃股票每日收盤行情
        url, progress = 'https://www.tpex.org.tw/web/stock/aftertrading/otc_quotes_no1430/stk_wn1430_result.php?l=zh-tw&o=htm&d=%s&se=EW&s=0,asc,0&_=%s' % (str(self.tradedaytoday.year-1911)+self.tradedaytoday.strftime('/%m/%d'), str(datetime.now().timestamp()*1000)[:13]), '上櫃股票每日收盤行情'
        temp_df = self._get_otcstkquote(url, progress)
        if temp_df is None:
            print('在處理 %s 的 %s 時沒有資料。' % (url, progress))
            return
        self.df = self.df.append(temp_df)
        print('已完成 %s 讀取及作業。' % progress)

        # 上市公司基本資料
        url, progress = 'http://mopsfin.twse.com.tw/opendata/t187ap03_L.csv', '上市公司基本資料'
        df = self._get_t187ap03L(url, progress)
        if df is None or df.empty:
            print('在處理 %s 的 %s 時沒有資料。' % (url, progress))
            return
        print('已完成 %s 讀取及作業。' % progress)

        # 上櫃公司基本資料
        url, progress = 'http://mopsfin.twse.com.tw/opendata/t187ap03_O.csv', '上櫃公司基本資料'
        temp_df = self._get_t187ap03O(url, progress)
        if temp_df is None or temp_df.empty:
            print('在處理 %s 的 %s 時沒有資料。' % (url, progress))
            return
        df = df.append(temp_df)
        print('已完成 %s 讀取及作業。' % progress)

        # 上市 ETF 基金基本資料彙總表上市股票型基金ETF基本資料彙總表
        url, progress = 'https://mops.twse.com.tw/server-java/t105sb02', '上市 ETF 基金基本資料彙總表'
        temp_df = self._get_t51sb11(url, progress)
        if temp_df is None or temp_df.empty:
            print('在處理 %s 的 %s 時沒有資料。' % (url, progress))
            return
        df = df.append(temp_df)
        print('已完成 %s 讀取及作業。' % progress)

        # 合併兩組資料
        self.df = self.df.join(df)

        # 上市上櫃國際證券辨識號碼表
        url, progress = 'http://isin.twse.com.tw/isin/C_public.jsp?', '上市上櫃國際證券辨識號碼表'
        print('正在讀取 %s 請稍候...' % progress)
        df = self._get_isincode(url, progress)
        if df is None or df.empty:
            print('在處理 %s 的 %s 時沒有資料。' % (url, progress))
            return
        print('已完成 %s 讀取及作業。' % progress)

        # 再次合併兩組資料
        self.df = self.df.join(df)

        try:
            self.df.abb_zhname.fillna('', inplace=True)
            self.df.name.fillna('', inplace=True)
            self.df.registration.fillna('', inplace=True)
            self.df.industry.fillna('', inplace=True)
            self.df.tax_ID.fillna('', inplace=True)
            self.df.found.fillna('', inplace=True)
            self.df.listing.fillna('', inplace=True)
            self.df.abb_enname.fillna('', inplace=True)
            self.df.website.fillna('', inplace=True)
            self.df.currency.fillna('TWD', inplace=True)
            self.df.parvalue.fillna(0, inplace=True)
            self.df.description.fillna('', inplace=True)
            self.df.type.fillna('', inplace=True)
            self.df.enname.fillna('', inplace=True)
            self.df.isincode.fillna(0, inplace=True)
            self.df.market.fillna('', inplace=True)
            self.df.reset_index(inplace=True)
        except Exception as e:
            print('匯整最終的資料時發生錯誤: %s' % (e))
            return

    def save_response_csv(self, filename, response_obj):
        with open(filename+'.csv', 'w', encoding='utf-8-sig') as f:
            f.write(response_obj)

    def _get_trade_day_today(self):
        base_time = 190000  # 下午7點190000
        today_time = self.todaytime
        today_date = today_time.date()
        closed_days = []

        print('正在連線證交所取得交易日期清單。')
        try:
            response = requests.get('https://www.twse.com.tw/holidaySchedule/holidaySchedule?response=csv', headers=self.tpex_headers)
        except Exception as e:
            print('程式結束。錯誤訊息：%s。因為連線交易日期網址的主機無法回應，連線嘗試失敗。' % e)
            sys.exit(1)
        response.encoding = 'big5'
        self.save_response_csv('證交所取得交易日期清單', response.text)
        holidays_csv = response.text.replace('"','')
        holidays_readlines = holidays_csv.split('\n')
        for holiday_line in holidays_readlines:
            holiday_column = holiday_line.split(',')
            if (len(holiday_column) > 1) and (holiday_column[4] != 'o'):
                holiday_value = holiday_column[1]  # [1] 為[日期]  = ['名稱', '日期', '星期', '說明', '備註(* : 市場無交易，僅辦理結算交割...。o : 交易日。)']
                if re.match(r'\d+月\d+日', holiday_value):
                    each_holiday_lines = re.findall(r'\d+月\d+日', holiday_value)
                    for each_holiday in each_holiday_lines:
                        _mon = each_holiday[:each_holiday.find('月')]
                        _day = each_holiday[each_holiday.find('月')+1:each_holiday.find('日')]
                        closed_days.append(datetime(today_date.year, int(_mon), int(_day)).date())
        nTimehms = int(today_time.strftime('%H%M%S'))
        day_offset = 0
        if nTimehms < base_time:
            day_offset = 1
        trading_day = today_date - relativedelta(days=day_offset)
        while (trading_day in closed_days) or (datetime.weekday(trading_day)>4):
            day_offset += 1
            trading_day = today_date - relativedelta(days=day_offset)
        print('今天目前為止最後一個有收盤資料的交易日為: %s' % trading_day)
        return trading_day

    # 上市股票每日收盤行情
    def _get_tseallbut0999(self, url, acquired_file):
        try:
            response = requests.get(url, headers=self.twse_headers)
        except Exception as e:
            print('從網頁讀取 %s 資料時發生錯誤。錯誤訊息: %s' % (acquired_file, e))
            return
        df = pd.read_html(StringIO(response.text), encoding='utf-8')[8]
        self.save_response_csv(acquired_file, response.text)
        try:
            df.columns = df.columns.get_level_values(2)
            df.drop(df.columns[2:].to_list(), axis=1, inplace=True)
            df.rename(columns={'證券代號': 'symbol_id', '證券名稱': 'abb_zhname'}, inplace=True)
            df.sort_values('symbol_id', inplace=True)
            df.set_index('symbol_id', inplace=True)
        except Exception as e:
            print('在處理網頁 %s 資料內容時發生錯誤。錯誤訊息: %s' % (acquired_file, e))
            return
        df.to_csv(acquired_file+'_df.csv', encoding='utf-8-sig')
        return df.copy()

    # 上櫃股票每日收盤行情
    def _get_otcstkquote(self, url, acquired_file):
        try:
            response = requests.get(url, headers=self.tpex_headers)
        except Exception as e:
            print('從網頁讀取 %s 資料時發生錯誤。錯誤訊息: %s' % (acquired_file, e))
            return
        df = pd.read_html(StringIO(response.text), encoding='utf-8')[0]
        self.save_response_csv(acquired_file, response.text)
        try:
            df.columns = df.columns.get_level_values(1)
            df.drop(df.columns[2:].to_list(), axis=1, inplace=True)
            df.rename(columns={'代號': 'symbol_id', '名稱': 'abb_zhname'}, inplace=True)
            df.sort_values('symbol_id', inplace=True)
            df.set_index('symbol_id', inplace=True)
        except Exception as e:
            print('在處理網頁 %s 資料內容時發生錯誤。錯誤訊息: %s' % (acquired_file, e))
            return
        df[:-1].to_csv(acquired_file + '_df.csv', encoding='utf-8-sig')
        return df[:-1].copy()

    # 上市公司基本資料
    def _get_t187ap03L(self, url, acquired_file):
        try:
            response = requests.get(url, headers=self.twse_headers)
        except Exception as e:
            print('從網頁讀取 %s 資料時發生錯誤。錯誤訊息: %s' % (acquired_file, e))
            return
        response.encoding = 'utf-8'
        self.save_response_csv(acquired_file, response.text)
        df = pd.read_csv(StringIO(response.text), encoding='utf-8', parse_dates=['成立日期', '上市日期'], dtype=str, date_parser=lambda dt: pd.to_datetime(dt, format='%Y%m%d'))
        df = df[['公司代號', '公司名稱', '公司簡稱', '外國企業註冊地國', '營利事業統一編號', '成立日期', '上市日期', '普通股每股面額', '英文簡稱', '網址']]
        try:
            df['外國企業註冊地國'] = df['外國企業註冊地國'].replace('－ ', '')
            df['外國企業註冊地國'] = df['外國企業註冊地國'].replace(' ', '')
            df['網址'] = df['網址'].apply(lambda url: url if urlparse(url).scheme else 'https://%s' % url)
            value_map = {'新台幣                 10.0000元': ['TWD', 10],
                '新台幣                  1.0000元': ['TWD', 1],
                '新台幣                  5.0000元': ['TWD', 5],
                '美元                  0.0010元': ['USD', 1],
                '不適用': ['', 0] ,
                '美金0.05元': ['USD', 0.05],
                '美金0.0050元': ['USD', 0.005],
                '泰銖1元': ['THB', 1],
                '港幣0.1元': ['HKD', 1],
                '0.01': ['', 0.01],
                '-': ['', 0],
                '泰銖10元': ['THB', 10],
                'HKD 0.1': ['HKD', 0.1],
                '0.1': ['', 0.1],
                '港幣0.01元': ['HKD', 0.01] }
            df = df.join(pd.DataFrame(df['普通股每股面額'].apply(lambda currency: value_map[currency]).tolist(),
                columns = ['幣別','新普通股每股面額']))
            df.drop(['公司簡稱', '普通股每股面額'], axis=1, inplace=True)
            df.sort_values('公司代號', inplace=True)
            df.rename(columns={
                '公司代號': 'symbol_id',
                '公司名稱': 'name',
                '外國企業註冊地國': 'registration',
                '營利事業統一編號': 'tax_ID',
                '成立日期': 'found',
                '上市日期': 'listing',
                '新普通股每股面額': 'parvalue',
                '幣別': 'currency',
                '英文簡稱': 'abb_enname',
                '網址': 'website'}, inplace=True)
            df.set_index('symbol_id', inplace=True)
        except Exception as e:
            print('整理 %s 資料的時發生錯誤: %s' % (acquired_file, e))
            return
        df.to_csv(acquired_file+'_df.csv', encoding='utf-8-sig')
        return df.copy()

    # 上櫃公司基本資料
    def _get_t187ap03O(self, url, acquired_file):
        try:
            response = requests.get(url, headers=self.twse_headers)
        except Exception as e:
            print('從網頁讀取 %s 資料時發生錯誤。錯誤訊息: %s' % (acquired_file, e))
            return
        response.encoding = 'utf-8'
        self.save_response_csv(acquired_file, response.text)
        df = pd.read_csv(StringIO(response.text), encoding='utf-8', parse_dates=['成立日期', '上市日期'], dtype=str, date_parser=lambda dt: pd.to_datetime(dt, format='%Y%m%d'))
        df = df[['公司代號', '公司名稱', '公司簡稱', '外國企業註冊地國', '營利事業統一編號', '成立日期', '上市日期', '普通股每股面額', '英文簡稱', '網址']]
        try:
            df['外國企業註冊地國'] = df['外國企業註冊地國'].replace('－ ', '')
            df['外國企業註冊地國'] = df['外國企業註冊地國'].replace(' ', '')
            df['網址'] = df['網址'].apply(lambda url: url if urlparse(url).scheme else 'https://%s' % url)
            value_map = {'新台幣                 10.0000元': ['TWD', 10],
                '新台幣                  1.0000元': ['TWD', 1],
                '新台幣                  5.0000元': ['TWD', 5],
                '美元                  0.0010元': ['USD', 1],
                '不適用': ['', 0] ,
                '美金0.0050元': ['USD', 0.005],
                '泰銖1元': ['THB', 1],
                '港幣0.1元': ['HKD', 1],
                '0.01': ['', 0.01],
                '-': ['', 0],
                '泰銖10元': ['THB', 10],
                'HKD 0.1': ['HKD', 0.1],
                '0.1': ['', 0.1],
                '港幣0.01元': ['HKD', 0.01] }
            df = df.join(pd.DataFrame(df['普通股每股面額'].apply(lambda currency: value_map[currency]).tolist(),
                columns = ['幣別','新普通股每股面額']))
            df.drop(['公司簡稱', '普通股每股面額'], axis=1, inplace=True)
            df.sort_values('公司代號', inplace=True)
            df.rename(columns={
                '公司代號': 'symbol_id',
                '公司名稱': 'name',
                '外國企業註冊地國': 'registration',
                '營利事業統一編號': 'tax_ID',
                '成立日期': 'found',
                '上市日期': 'listing',
                '新普通股每股面額': 'parvalue',
                '幣別': 'currency',
                '英文簡稱': 'abb_enname',
                '網址': 'website'}, inplace=True)
            df.set_index('symbol_id', inplace=True)
        except Exception as e:
            print('整理 %s 資料時發生錯誤: %s' % (acquired_file, e))
            return
        df.to_csv(acquired_file + '_df.csv', encoding='utf-8-sig')
        return df.copy()

    # 上市股票型基金ETF基本資料彙總表
    def _get_t51sb11(self, url, acquired_file):
        def dt_func(dt):
            dt_split = [int(col) for col in dt.split('/')]
            dt_year = dt_split[0] + 1911
            dt_month = dt_split[1]
            dt_day = dt_split[2]
            return pd.to_datetime('%04d%02d%02d' % (dt_year, dt_month, dt_day), format='%Y%m%d')

        try:
            response = requests.post(url, headers=self.twse_headers, data={'firstin': 'true', 'step': '10', 'filename': 't51sb11.csv'})
        except Exception as e:
            print('從網頁讀取 %s 資料時發生錯誤。錯誤訊息: %s' % (acquired_file, e))
            return
        response.encoding = 'dbcs'
        self.save_response_csv(acquired_file, response.text)
        df = pd.read_csv(StringIO(response.text), encoding='utf-8', dtype=str) #, date_parser=dt_func) parse_dates=['成立日期', '上市日期'],
        df = df[['基金代號', '基金名稱', '標的指數/追蹤指數名稱', '基金類型', '英文名稱', '統一編號', '上市日期', '成立日期']]
        df.drop_duplicates(inplace=True)
        df['基金代號'] = df['基金代號'].str.replace('"', '')
        df['基金代號'] = df['基金代號'].str.replace('=', '')
        df.drop(df[df['基金代號']=='基金代號'].index, inplace=True)
        df['上市日期'] = df['上市日期'].apply(dt_func)
        df['成立日期'] = df['成立日期'].apply(dt_func)
        df.sort_values('基金代號', inplace=True)
        try:
            df.rename(columns={
                '基金代號': 'symbol_id',
                '基金名稱': 'name',
                '標的指數/追蹤指數名稱': 'description',
                '基金類型': 'type',
                '英文名稱': 'enname',
                '統一編號': 'tax_ID',
                '上市日期': 'listing',
                '成立日期': 'found'}, inplace=True)
        except Exception as e:
            print('整理 %s 資料時發生錯誤: %s' % (acquired_file, e))
            return
        df = df.groupby('symbol_id').last()
        df.to_csv(acquired_file + '_df.csv', encoding='utf-8-sig')
        return df.copy()

    # 上市上櫃國際證券辨識號碼表
    def _get_isincode(self, url, acquired_file):
        compose_data = []
        for strMode in ({'2':'上市'}, {'4':'上櫃'}):
            for strKey, strValue in strMode.items():
                inner_url = '%sstrMode=%s' % (url, strKey)
                try:
                    response = requests.get(inner_url, headers=self.twse_headers)
                except Exception as e:
                    print('嘗試取得 %s 資料時發生錯誤: %s' % (acquired_file, e))
                    return
                response.encoding ='dbcs'
                self.save_response_csv(acquired_file+strValue, response.text)
                bs4soup = BeautifulSoup(response.text, 'lxml')
                h4table = bs4soup.find('table', {'class': 'h4'})
                lastRow = None
                for row in h4table.find_all('tr'):
                    data = []
                    for col in row.find_all('td'):
                        each_col = col.text.strip().split('\u3000')
                        for ecol in each_col:
                            data.append(ecol)
                    if data and data[0] == '有價證券代號及名稱':
                        data[0] = '有價證券名稱'
                        data.insert(0, '有價證券代號')
                        data.append('證券類別')
                        if strKey == '2':
                            compose_data.append(data)
                        continue
                    if len(data) == 1:
                        lastRow = data
                        if lastRow[0] in ('股票', '特別股', 'ETF', '臺灣存託憑證'):
                            lastRow[0] = '%s%s' % (strValue, lastRow[0])
                        continue
                    if lastRow:
                        compose_data.append(data + lastRow)
                    else:
                        print('error!')
                        break
        try:
            df = pd.DataFrame(compose_data[1:], columns=compose_data[0])
            df.sort_values('有價證券代號', inplace=True)
            df.drop(['有價證券名稱', '上市日', '市場別', 'CFICode', '備註'], axis=1, inplace=True)
            df.rename(columns={
                '有價證券代號': 'symbol_id',
                '國際證券辨識號碼(ISIN Code)': 'isincode',
                '證券類別': 'market',
                '產業別': 'industry'
                }, inplace=True)
            df.set_index('symbol_id', inplace=True)
        except Exception as e:
            print('整理 %s merge 資料時發生錯誤: %s' % (acquired_file, e))
            return
        df.to_csv(acquired_file+'_df.csv', encoding='utf-8-sig')
        return df.copy()

# === 建立 security 資料表內容 ===
security_data = security_crawler()
if security_data.df is None or security_data.df.empty:
    print('嘗試從網站讀取資料但資料是錯的。')
    sys.exit(1)
print('已讀取下載網路資料並已整理成 %d 筆資料。正在寫入資料庫 security 資料表 ...' % len(security_data.df))
try:
    filename = '於 %s 取得的 security 清單.csv' % datetime.now().strftime('%Y%m%d_%H%M%S')
    security_data.df.to_csv(filename, encoding='utf-8-sig')
except Exception as e:
    print('將匯整清單寫入本機時發生錯誤: %s' % (e))
    sys.exit(1)
print('已儲存 %s 檔案。完成！' % filename)
