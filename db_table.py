# -*- coding:utf-8 -*- 
import sys
if '..' not in sys.path:
    sys.path.append('..')
from future_mysql.dbBase import DB_BASE

from sqlalchemy import Column, Integer, String, DateTime, Numeric, Index, Float
from sqlalchemy import Table

import pandas as pd

trans = lambda x: x.encode('utf8') if isinstance(x,unicode) else x

class CiticBank(DB_BASE):

    def __init__(self, db_name='citic_bank', table_name=None):
        super(CiticBank, self).__init__(db_name)
        self.table_struct = None
        self.col_sizes = []
        
    def create_table(self):
        if self.table_struct is not None:
            self.table_struct = self.quick_map(self.table_struct)

    def check_table_exist(self):
        if self.table_struct is not None:
            return self.table_struct.exists()
        else:
            raise Exception("no table specified")
        
    def get_row_counts(self):
        session = self.get_session()
        n = session.query(self.table_struct).count()
        session.close()
        return n
    
    def get_col_names(self):
        return [ trans(i) for i in self.get_column_names(self.table_struct)]
    
    def get_col_length(self):
        return len(self.get_column_names(self.table_struct))
    
    def get_col_sizes(self):
        return self.col_sizes

def import_reserve():
    infile = r'/media/xudi/coding/支付/深圳中信/5.4/上午/公司银行部/reserve.csv'
    df = pd.read_csv(infile,encoding = 'utf8',index_col = 0)
    print df.head()
    db_obj = CiticBank()
    df.to_sql('reserve',db_obj.engine,chunksize = 10240,if_exists='append')
    print 'finished'
    
def import_online_account():
    infile = r'/media/xudi/coding/支付/深圳中信/5.7/2类户/线上开立2类户.csv'
    df = pd.read_csv(infile,encoding = 'utf8',index_col = None)
    print len(df)
    db_obj = CiticBank()
    df.to_sql('personal_account_second_online',db_obj.engine,chunksize = 10240,if_exists='append')
    print 'finished'
    
def import_offline_account():
    infile = r'/media/xudi/coding/支付/深圳中信/5.7/2类户/线下开立1,2类户_1.csv'
    df = pd.read_csv(infile,encoding = 'utf8',index_col = None)
    print len(df)
    db_obj = CiticBank()
    df.to_sql('personal_account_second_offline',db_obj.engine,chunksize = 10240,if_exists='append')
    infile = r'/media/xudi/coding/支付/深圳中信/5.7/2类户/线下开立1,2类户_2.csv'
    df = pd.read_csv(infile,encoding = 'utf8',index_col = None)
    print len(df)
    db_obj = CiticBank()
    df.to_sql('personal_account_second_offline',db_obj.engine,chunksize = 10240,if_exists='append')
    print 'finished'

def import_reserve_trade():
    infile = r'/media/xudi/coding/支付/深圳中信/5.7/附件6-备付金账户流水20180101-20180107.csv'
    df = pd.read_csv(infile,encoding = 'utf8',index_col = None)
    df.columns = [ i.strip() for i in df.columns]
    print df.columns
    db_obj = CiticBank()
    df.to_sql('reserve_trade',db_obj.engine,chunksize = 10240,if_exists='append')
    print 'finished'

def import_citic_company_account():
    infile = r'/media/xudi/coding/支付/深圳中信/5.7/运营管理部/单位结算账户信息(账户管理系统）/开户_20180505_汇总.xls'
    df = pd.read_excel(infile,encoding = 'utf8',index_col = None)
    df.columns = [ i.strip() for i in df.columns]
    print df.columns
    db_obj = CiticBank()
    df.to_sql('citic_company_account',db_obj.engine,chunksize = 10240,if_exists='append')
    print 'finished'
    
def import_pbc_company_account_log():
    infiles = [ r'/media/xudi/coding/支付/深圳中信/5.8/早上/20160503-20180503备案类数据记录（人行账户管理系统）/一般账户备案20160503-20170503.xls',
                r'/media/xudi/coding/支付/深圳中信/5.8/早上/20160503-20180503备案类数据记录（人行账户管理系统）/一般账户备案20170504-20180503.xls'
               ]
    for infile in infiles:
        print infile
        df = pd.read_excel(infile,encoding = 'utf8',index_col = None)
        df.columns = [ i.strip() for i in df.columns]
        print df.columns
        db_obj = CiticBank()
        df.to_sql('pbc_company_account_log',db_obj.engine,chunksize = 10240,if_exists='append')
    print 'finished'


def import_pbc_account_system_status():
    infile = r'/media/xudi/coding/支付/深圳中信/5.7/运营管理部/单位结算账户信息(账户管理系统）/开户_20180505_汇总.xls'
    df = pd.read_excel(infile,encoding = 'utf8',index_col = None)
    df.columns = [ i.strip() for i in df.columns]
    print df.columns
    db_obj = CiticBank()
    df.to_sql('pbc_account_system_status',db_obj.engine,chunksize = 10240,if_exists='append')
    print 'finished'
    
    
if __name__ == '__main__':
    import_pbc_account_system_status()
    
    