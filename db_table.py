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

def import_core(infile,table_name):
    if infile.endswith('.csv'):
        df = pd.read_csv(infile,encoding = 'utf8',index_col = None)
    else:
        df = pd.read_excel(infile,encoding = 'utf8',index_col = None)
    df.columns = [ i.strip() for i in df.columns]
    print df.columns
    db_obj = CiticBank()
    df.to_sql(table_name,db_obj.engine,chunksize = 10240,if_exists='append')
    print 'finished'

def import_reserve():
    infile = r'/media/xudi/coding/支付/深圳中信/5.4/上午/公司银行部/reserve.csv'
    import_core(infile,'reserve')
    
def import_online_account():
    infile = r'/media/xudi/coding/支付/深圳中信/5.7/2类户/线上开立2类户.csv'
    import_core(infile,'personal_account_second_online')
    
def import_offline_account():
    infile = r'/media/xudi/coding/支付/深圳中信/5.7/2类户/线下开立1,2类户_1.csv'
    import_core(infile,'personal_account_second_offline')
    infile = r'/media/xudi/coding/支付/深圳中信/5.7/2类户/线下开立1,2类户_2.csv'
    import_core(infile,'personal_account_second_offline')

def import_reserve_trade():
    infile = r'/media/xudi/coding/支付/深圳中信/5.7/附件6-备付金账户流水20180101-20180107.csv'
    import_core(infile,'reserve_trade')

def import_citic_company_account():
    infile = r'/media/xudi/coding/支付/深圳中信/5.7/运营管理部/单位结算账户信息(账户管理系统）/开户_20180505_汇总.xls'
    import_core(infile,'citic_company_account')
    
def import_pbc_company_account_log():
    infiles = [ r'/media/xudi/coding/支付/深圳中信/5.8/早上/20160503-20180503备案类数据记录（人行账户管理系统）/一般账户备案20160503-20170503.xls',
                r'/media/xudi/coding/支付/深圳中信/5.8/早上/20160503-20180503备案类数据记录（人行账户管理系统）/一般账户备案20170504-20180503.xls'
               ]
    for infile in infiles:
        print infile
        import_core(infile,'pbc_company_account_log')
    print 'finished'

def import_pbc_account_system_status():
    infile = r'/media/xudi/coding/支付/深圳中信/5.7/运营管理部/单位结算账户信息(账户管理系统）/开户_20180505_汇总.xls'
    import_core(infile,'pbc_account_system_status')
    
def import_citic_bank_change_status():    
    infile = r'/media/xudi/coding/支付/深圳中信/5.7/运营管理部/核心系统开户销户变更数据/变更.xls'
    table_name = r'citic_account_change_status'
    import_core(infile,table_name)
    
def import_pbc_banck_change_status():
    infile = r'/media/xudi/coding/支付/深圳中信/5.8/早上/单位结算账户信息(账户管理系统）/变更_20180505_汇总.xls'
    table_name = r'pbc_account_change_status'
    import_core(infile,table_name)

def import_citic_bank_delete_status():    
    infile = r'/media/xudi/coding/支付/深圳中信/5.8/早上/核心系统开户销户变更数据/核心开销户.xlsx'
    table_name = r'citic_account_delete_status'
    import_core(infile,table_name)
    
def import_pbc_banck_delete_status():
    infile = r'/media/xudi/coding/支付/深圳中信/5.8/早上/单位结算账户信息(账户管理系统）/销户20180505_汇总.xls'
    table_name = r'pbc_account_delete_status'
    import_core(infile,table_name)
    
if __name__ == '__main__':
    import_pbc_banck_change_status()
    import_pbc_banck_delete_status()
    
    