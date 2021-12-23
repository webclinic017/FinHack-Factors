from library.config import config
from library.mysql import mysql
from library.astock import AStock
import pandas as pd
import os
from importlib import import_module
import re
class compute():
 
        

    def computeAll(db):
        
        mysql.exec("drop table if exists finhack_factors",'factors')
        df_code= AStock.getStockCodeList(db)
        df_all=df_code.apply(compute.computeAllFactorsByStockList,axis=1)
       #


    def computeList(list_name,factor_list,code_list=pd.DataFrame(),db='tushare'):
        if code_list.empty:
            code_list=AStock.getStockCodeList('tushare')
        
        code_list=list(code_list['ts_code'])
 
            
        for ts_code in code_list:
            print(ts_code)
            df_price=AStock.getStockDailyPriceByCode(ts_code,db)
            df_result=df_price.copy()
            df=pd.DataFrame()
            for factor_name in factor_list:
                # print(factor_list)
                # exit();
                #factor_name=row['name']
                factor=factor_name.split('_')
                if not factor[0] in df.columns:
                    df=compute.computeFactorByStock(ts_code,factor_name,df_price,'factors')
                else:
                    #print(factor[0])
                    pass
                if(df_price.empty):
                    continue
                df_result[factor_name]=df[factor[0]]
            engine=mysql.getDBEngine('factors')    
            res = df_result.to_sql('factors_'+list_name, engine, index=False, if_exists='append', chunksize=5000)
        pass


    def getFunctionMapByFactorName(factor_name):
        #print("getFunctionByFactorName:"+factor_name)
        factor=factor_name.split('_')
        factor_filed=factor[0]
        function=mysql.selectToList('select * from function_map where factor_filed="'+factor_filed+'"','factors')
        #print('select * from function_map where factor_filed="'+factor_filed+'"')
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
        
        #print(flist)
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
        #df_result[factor_name]=df[factor[0]]
        return df
        

    def computeAllFactorsByStockList(code_list,db='tushare'):
        factor_list=mysql.selectToList('select * from factor_list','factors')
        compute.computeList('all',factor_list,code_list,db)
        pass
        
   
    def computeStockFactorByfiled(filed):
        pass