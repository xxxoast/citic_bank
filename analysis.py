# -*- coding:utf-8 -*- 

from db_table import CiticBank
import pandas as pd

def date_trans(x):
    if x is not None:
        t = x.strip().split()
        hour,min,sec = [ int(i) for i in t[0].split(':')] 
        if t[1] == u'下午':
            hour = hour + 12 
        return hour*10000 + min*100 + sec
    return x
    
def ainongyizhan_analysis():
    table_name = 'tmp_ainongyizhan_everyday_summary'
    dbapi = CiticBank()
    df = pd.read_sql_table(table_name,dbapi.engine)
    df['credit_last_timestamp'] = df['credit_last_timestamp'].apply(date_trans)
    df['debt_last_timestamp'] = df['debt_last_timestamp'].apply(date_trans)
    df = df.fillna(0)
    df[df.columns[1:]] = df[df.columns[1:]].astype(int)
    df['residual_of_day'] = -1
    df['residual_timestamp'] = -1
    tstamp_diff = (df['credit_last_timestamp'] - df['debt_last_timestamp']).apply(lambda x:8 if x < 0 else 4)
    credit_index = tstamp_diff.apply(lambda x:x == 4)
    debt_index = tstamp_diff.apply(lambda x:x == 8)
    df.loc[credit_index,'residual_timestamp'] = df.loc[credit_index,'credit_last_timestamp']
    df.loc[debt_index,'residual_timestamp'] = df.loc[debt_index,'debt_last_timestamp']
    df.loc[credit_index,'residual_of_day'] = df.loc[credit_index,'credit_residual']
    df.loc[debt_index,'residual_of_day'] = df.loc[debt_index,'debt_residual']
    df.to_sql('tmp_ainongyizhan_everyday_summary_processed',dbapi.engine)
    print 'finished'

if __name__ == '__main__':
    ainongyizhan_analysis()
    