# -*- coding:utf-8 -*- 
import sys
from bcolz.ctable import cols
if '..' not in sys.path:
    sys.path.append('..')
from future_mysql.dbBase import DB_BASE

from sqlalchemy import Column, Integer, String, DateTime, Numeric, Index, Float, NVARCHAR, BigInteger
from sqlalchemy import Table
import os
import pandas as pd

trans = lambda x: x.encode('utf8') if isinstance(x,unicode) else x

def mapping_df_types(df): 
    dtypedict = {} 
    for i, j in zip(df.columns, df.dtypes): 
        if i == u'账户':
            dtypedict.update({i: NVARCHAR(length=32)}) 
#         if "object" in str(j): 
#             dtypedict.update({i: NVARCHAR(length=255)}) 
#         if "float" in str(j): 
#             dtypedict.update({i: Float(precision=2, asdecimal=True)})
#         if "int" in str(j): 
#             dtypedict.update({i: Integer()}) 
    return dtypedict

class CiticBank(DB_BASE):

    def __init__(self, db_name='citic_bank_tmp', table_name=None):
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

class reserve_trade_all(CiticBank):
    
    def __init__(self, db_name='citic_bank', table_name='reserve_trade_all'):
        super(reserve_trade_all, self).__init__(db_name)
        self.table_struct = None
        if table_name is not None:
            self.table_name = table_name
            self.table_struct = Table(
                table_name, self.meta,
                #
                Column('index',Integer,primary_key = True,autoincrement=True),
                Column('citic_custom_id',Integer),
                Column('origin_account_number', String(32),index = True),
                Column('origin_account_name', String(128)),
                Column('trade_id',Integer),
                Column('date', String(32)),
                Column('timestamp', String(40)),
                #
                Column('operation_id',String(32)),
                Column('direction',String(6)),
                Column('turnover',Float),
                Column('residual',Float),
                Column('opponent_account_number', String(32),index = True),
                Column('opponent_account_name', String(64)),
                Column('opponent_bank_name', String(128)),
                Column('abstract',String(128)),
            )

def import_core(infile,table_name,encoding = 'utf8',if_exists='append'):
    if infile.endswith('.csv') or infile.endswith('.txt'):
        df = pd.read_csv(infile,encoding = encoding,index_col = None)
    else:
        df = pd.read_excel(infile,encoding = encoding,index_col = None)
    df.columns = [ i.strip() for i in df.columns]
    print df.columns
    db_obj = CiticBank()
    dtypedict = mapping_df_types(df)
    df.to_sql(table_name,db_obj.engine,chunksize = 10240,if_exists=if_exists,dtype=dtypedict)
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
   
def import_reserve_trade_all():   
    root_path = r'/media/xudi/coding/支付/深圳中信/5.8/中午/支付机构备付金交易流水20160503-20180503'
    csvs = [ i for i in os.listdir(root_path) if i.endswith('.txt') ]
    dbapi = reserve_trade_all()
    dbapi.create_table()
    cols = dbapi.get_col_names()
    for infile in csvs:
        print infile
        df = pd.read_csv(os.path.join(root_path,infile),encoding = 'cp936',index_col = None)
        df.columns = cols[1:]
        df.to_sql(dbapi.table_name,dbapi.engine,if_exists='append',chunksize=10240,index = False) 
        
def import_cross_validation_1():   
    table_name = r'cross_validation'
    infile = r'/media/xudi/coding/支付/深圳中信/5.9/联网核查记录/20171101-20180228联网核查记录（new）.csv'
    import_core(infile,table_name,encoding = 'utf8')        
 
def import_cross_validation_2():   
    table_name = r'cross_validation_2018'
    infile = r'/media/xudi/coding/支付/深圳中信/5.9/联网核查记录/20180301-20180507联网核查记录（new）.csv'
    import_core(infile,table_name,encoding = 'utf8')     
       
def import_273():
    root_dir = r'/media/xudi/coding/支付/深圳中信/5.11/代收付/273流水'
    xlss = [i for i in os.listdir(root_dir) if i.endswith('xlsx')]       
    table_name = r'273_trade'
    for xls in xlss:
        print xls
        infile = os.path.join(root_dir,xls)
        import_core(infile,table_name)
          
if __name__ == '__main__':
    import_273()
    
    