# 整合版 即時 台灣 上市上櫃股票清單 下載程式

分享經過反覆驗證和推敲出來的整合版即時台股的上市上櫃的股票清單下載程式。比起網路上的版本就是多了很多驗證和整合的部份。至少可以確定最後下載的版本是對的及內容是正確的。

整合的資料包含下列資料:

# 上市股票每日收盤行情
https://www.twse.com.tw/exchangeReport/MI_INDEX?response=html&date=%s&type=ALLBUT0999


# 上櫃股票每日收盤行情
https://www.tpex.org.tw/web/stock/aftertrading/otc_quotes_no1430/stk_wn1430_result.php?l=zh-tw&o=htm&d=%s&se=EW&s=0,asc,0&_=


# 上市公司基本資料
http://mopsfin.twse.com.tw/opendata/t187ap03_L.csv


# 上櫃公司基本資料
http://mopsfin.twse.com.tw/opendata/t187ap03_O.csv


# 上市 ETF 基金基本資料彙總表
https://mops.twse.com.tw/server-java/t105sb02


# 上市上櫃國際證券辨識號碼表
http://isin.twse.com.tw/isin/C_public.jsp?


# 股市交易日期清單
https://www.twse.com.tw/holidaySchedule/holidaySchedule?response=csv

# License
GNU GENERAL PUBLIC LICENSE 3.0
