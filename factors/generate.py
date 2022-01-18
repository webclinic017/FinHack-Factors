#生成py文件
from library.config import config
from library.mysql import mysql
import pandas as pd
import os

class generate:
    # 命名空间，因子大类，所有因子函数写到这个文件
    def generateAllNameSpace():
        namespace_list=mysql.selectToDf('select * from factor_namespace','factors')
        for i,row in namespace_list.iterrows():
            namespace=row['namespace']
            filename=os.path.dirname(__file__)+"/namespace/"+namespace+".py"
            #common_import=row['common_import']
            common_code=row['common_code']
            if common_code==None:
                common_code=''
            # if common_import==None:
            #     common_import=''
            print('generated: '+filename)
            common_code=common_code.replace("\t",'    ')
            fo = open(filename, "w")
            fo.write(common_code+"\n\n")
            fo.write("class "+namespace+":\n\n")
            #fo.write(common_code+"\n")
            fo.close()
        

    # 所有函数写入命名空间
    def generateAllFunction():
        mysql.truncateTable('function_map','factors')
        function_list=mysql.selectToDf('select * from factor_function','factors')
        for i,row in function_list.iterrows():
            namespace=row['namespace']
            filename=os.path.dirname(__file__)+"/namespace/"+namespace+".py"
            function=row['function']
            name=row['name']
            function=function.replace("\t",'    ')
            lines=function.split("\n"); 
            print('generated: '+filename+","+name)
            fo = open(filename, "a")
            for line in lines:
                fo.write("    "+line+"\n")
            fo.write("\n")
            fo.close()
            
            generate.generateFunctionMap(row)
            
    
    
    def generateFunctionMap(row):
        factor_fields=row['return_fileds'].split(',')
        for factor_field in factor_fields:
            #print('-'+factor_field)
            fmap={
                'factor_filed':factor_field,
                'func_id':row['id'],
                'func_namespace':row['namespace'],
                'func_name':row['name']
            }
            fmap=pd.DataFrame(fmap,index=[0])
            engine=mysql.getDBEngine('factors')
            fmap.to_sql('function_map', engine, index=False, if_exists='append', chunksize=5000)
        