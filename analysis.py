# -*- coding:utf-8 -*- 

from db_table import CiticBank
import pandas as pd

def date_diff():
    table_name = 'yibanhu_diff'
    dbapi = CiticBank()
    df = pd.read_sql_table(table_name,dbapi.engine)
    df = df[df[u'状态'] == u'正常']
    df[u'开户日期'] = df[u'开户日期'].apply(pd.to_datetime)
    df[u'备案日期'] = df[u'备案日期'].apply(pd.to_datetime)
    df[u'延迟日期'] = (df[u'备案日期'] - df[u'开户日期']).apply(lambda x:x.days)
    print df.head()

if __name__ == '__main__':
    date_diff()