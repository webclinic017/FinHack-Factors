from library.config import config
from library.mysql import mysql
from library.astock import AStock
import pandas as pd
import os
from importlib import import_module
import re
from pandarallel import pandarallel
class compute():
 
        

    def computeAll(db):
        pandarallel.initialize(progress_bar=True)
        mysql.exec("drop table if exists factors_all_tmp",'factors')
        df_code= AStock.getStockCodeList(db)
        df_all=df_code.parallel_apply(compute.computeAllFactorsByStock,axis=1)
        compute.putData('factors_all','factors_all_tmp','factors')

    def computeOne(list_name,factor_list,ts_code,db='tushare'):
            ts_code=ts_code['ts_code']
            df_price=AStock.getStockDailyPriceByCode(ts_code,db)
            df_result=df_price.copy()
            #df=pd.DataFrame()

            for row in factor_list:
                df=pd.DataFrame() #总冲突，放下来每次重新计算吧
                
                # exit();
                factor_name=row['name']
                factor=factor_name.split('_')
                df_price_copy=df_price.copy()
                if not factor_name in df.columns:#原来是factor[0]，同名函数冲突，暂时改成函数名
                    df=compute.computeFactorByStock(ts_code,factor_name,df_price_copy,'factors')
                else:
                    #print(factor[0])
                    pass
                if(df_price.empty):
                    continue
                df_result[factor_name]=df[factor_name]
                engine=mysql.getDBEngine('factors') 
                res = df_result.to_sql('factors_'+list_name+'_tmp', engine, index=False, if_exists='append', chunksize=5000)
 
                #print(df_result)
 
                return df_result

    def computeList(list_name,factor_list,code_list=pd.DataFrame(),db='tushare'):
        if code_list.empty:
            code_list=AStock.getStockCodeList('tushare')
            code_list=list(code_list['ts_code'])

        for ts_code in code_list:
            df_result=compute.computeOne(list_name,factor_list,ts_code=pd.DataFrame(),db='tushare')
            #df['date']=df['trade_date'].str[0:4]+'-'+df['trade_date'].str[4:6]+'-'+df['trade_date'].str[6:8]

        compute.putData('factors_'+list_name,'factors_'+list_name+'_tmp','factors')
        pass


    def putData(table,tmptable,db='factors'):
        index_list=['ts_code','end_date','trade_date']
        for index in index_list:
            sql="CREATE INDEX "+index+" ON "+tmptable+" ("+index+"(10)) "
            mysql.exec(sql,db)        
        mysql.exec('rename table '+table+' to '+table+'_old;','factors');
        mysql.exec('rename table '+tmptable+' to '+table+';','factors');
        mysql.exec("drop table if exists "+table+'_old','factors')
    


    def getFunctionMapByFactorName(factor_name):
        #print("getFunctionByFactorName:"+factor_name)
        factor=factor_name.split('_')
        factor_filed=factor[0]
        function=mysql.selectToList('select * from function_map where factor_filed="'+factor_filed+'"','factors')
        # print('select * from function_map where factor_filed="'+factor_filed+'"')
        # print(function)
        return function[0]


    def getCodeByFunctionName(function_name):
        #print("getFunctionByFactorName:"+factor_name)
        function=mysql.selectToList('select * from factor_function where name="'+function_name+'"','factors')
        #print('select * from function_map where factor_filed="'+factor_filed+'"')
        return function[0]



    def computeFactorByStock(ts_code,factor_name,df_price,db='tushare'):
        if(df_price.empty):
            df_price=AStock.getStockDailyPriceByCode(ts_code,'tushare')
            df_result=df_price.copy()
        if(df_price.empty):
            return pd.DataFrame()
        
        func_map=compute.getFunctionMapByFactorName(factor_name)
   
        function=compute.getCodeByFunctionName(func_map['func_name'])
        code=function['function']
        df_result=df_price.copy()
        pattern = re.compile(r"df\[\'(\w*?)\'\]")   # 查找数字
        flist = pattern.findall(code)
        rlist=function['return_fileds'].split(',')
        
        flist=list(set(flist) - set(rlist))
        
        #print("factor_name:"+factor_name)
        
         
        for f in flist:
            if not f in df_price.columns:
                df_price=compute.computeFactorByStock(ts_code,f,df_price,db)
                
 
        
        factor=factor_name.split('_')
        
        module = getattr(import_module('factors.namespace.'+func_map['func_namespace']), func_map['func_namespace'])
        func=getattr(module,func_map['func_name'])
 
 
        for i in range(1,len(factor)):
            factor[i]=int(factor[i])
        df=func(df_price,factor)
        # print(df)
        # print(factor_name)
        # print(factor[0])
        #exit()
        df[factor_name]=df[factor[0]]
        
 
        return df
        

    def computeAllFactorsByStock(ts_code,db='tushare'):
        factor_list=mysql.selectToList('select * from factor_list','factors')
        compute.computeOne('all',factor_list,ts_code,db=db)
        pass
        
   
    def computeStockFactorByfiled(filed):
        pass