# -*- coding:utf-8 -*- 

from db_table import CiticBank
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

def date_trans(x):
    if x is not None:
        t = x.strip().split()
        hour,min,sec = [ int(i) for i in t[0].split(':')] 
        if t[1] == u'下午':
            hour = hour + 12 
        return hour*10000 + min*100 + sec
    return x
    
def processed_table_generator(in_table_name,out_table_name):
    dbapi = CiticBank()
    df = pd.read_sql_table(in_table_name,dbapi.engine)
    df['credit_last_timestamp'] = df['credit_last_timestamp'].apply(date_trans)
    df['debt_last_timestamp'] = df['debt_last_timestamp'].apply(date_trans)
    df = df.fillna(0)
    df[df.columns[1:]] = df[df.columns[1:]].astype(int)
    df['residual_of_day'] = -1
    df['residual_timestamp'] = -1
    df['redisual_last_trade_index'] = -1
    tstamp_diff = (df['credit_last_trade_index'] - df['debt_last_trade_index']).apply(lambda x:8 if x < 0 else 4)
    credit_index = tstamp_diff.apply(lambda x:x == 4)
    debt_index = tstamp_diff.apply(lambda x:x == 8)
    df.loc[credit_index,'residual_timestamp'] = df.loc[credit_index,'credit_last_timestamp']
    df.loc[debt_index,'residual_timestamp'] = df.loc[debt_index,'debt_last_timestamp']
    df.loc[credit_index,'redisual_last_trade_index'] = df.loc[credit_index,'credit_last_trade_index']
    df.loc[debt_index,'redisual_last_trade_index'] = df.loc[debt_index,'debt_last_trade_index']
    df.loc[credit_index,'residual_of_day'] = df.loc[credit_index,'credit_residual']
    df.loc[debt_index,'residual_of_day'] = df.loc[debt_index,'debt_residual']
    df['net_out_amount'] = df['credit_turnover'] - df['debt_turnover']
    df['expected_residual'] = -1
    df['expected_residual'].values[1:] = df['residual_of_day'].values[:-1] - df['net_out_amount'].values[1:]
    df['expected_residual'].values[0] = df['residual_of_day'].values[0]
    df.to_sql(out_table_name,dbapi.engine)
    df['expected_residual'].cumsum().plot()
    df['residual_of_day'].cumsum().plot()
    plt.show()
    print 'finished'

def ainongyizhan_analysis():
    table_name = 'tmp_ainongyizhan_everyday_summary_processed'
    dbapi = CiticBank()
    df = pd.read_sql_table(table_name,dbapi.engine)
    diff = ( df['expected_residual'].cumsum() - df['residual_of_day'].cumsum())
    diff.index = df['date']
    diff.plot()
    plt.show()

def linminlong():
    table_name = '273_trade'
    dbapi = CiticBank()
    df = pd.read_sql_table(table_name,dbapi.engine)
    print len(df)
    df[u'交易时间'] = df[u'交易时间'].apply(date_trans).apply(int)
    df[u'weekday'] = df[u'交易日期'].apply(pd.to_datetime).apply(lambda x:x.weekday()).astype(np.int)
    tobechecked = np.logical_and( (df[u'交易时间'] < 170000 ) ,  (df[u'交易时间'] > 90000) )
    tobechecked = np.logical_and( df[u'weekday'] < 5, tobechecked )
    df = df.loc[ tobechecked ]
    print len(df)
    df.to_sql('273_trade_special',dbapi.engine,if_exists = 'fail')
    
def corp_dialy():
    corpname = 'kayou'
    in_table_name = 'tmp_{}_everyday_summary'.format(corpname)
    out_table_name = 'tmp_{}_everyday_summary_processed'.format(corpname)
    processed_table_generator(in_table_name,out_table_name)

if __name__ == '__main__':
    corp_dialy()
    