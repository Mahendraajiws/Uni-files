#!/usr/bin/env python
# coding: utf-8

# In[96]:


import pandas as pd
import pandas as pd
import numpy as py
import glob
import time
import os
from os import walk
from sqlalchemy import create_engine, MetaData, Table, select
from six.moves import urllib
import sqlalchemy

path_unloading = r'E:\Mahendra\INTERNSHIP\UNILEVER\JOB\Python for Dispatch and Unloading Compliance\Dispatch and Unloading Compliance\Unloading Week 23'
for file_unloading, dtbase_unloading, files_unloading in os.walk(path_unloading):
    for filename_unloading in files_unloading:
        print(filename_unloading)
        data_in_file_unloading = pd.read_excel(path_unloading+'/'+filename_unloading,sheet_name='Week 23',                                                index_col=None,header=1).fillna(0)
        data_in_file_unloading.rename(columns={'Unloading Capacity (PDP)':'PDP',                                                'Total shipments available at distributor (during the day)':                                                'AVAILABLE SHIPMENTS','Clear Final (Final Unload)':'CLEAR FINAL',                                                'Semesta 2':'SEMESTA 2','Comply 2':'COMPLY 2'},inplace=True)
        cols = [1,6,8,10,11,12,13,14,15,16,17,18]
        data_in_file_unloading.drop(data_in_file_unloading.columns[cols],axis=1,inplace=True)
        year_in_file_unloading = filename_unloading[-18:-14]
        week_in_file_unloading = filename_unloading[-12:-10]
        file_name_in_file_unloading = filename_unloading[-18:-5]
        print(year_in_file_unloading,week_in_file_unloading, file_name_in_file_unloading)
        df_raw = pd.DataFrame(data_in_file_unloading)
        
        df=df_raw.groupby(['Distributor Code'])['SEMESTA 2','COMPLY 2'].sum().reset_index()
        df.rename(columns={'SEMESTA 2':'TOTAL SEMESTA 2','COMPLY 2':'TOTAL COMPLY 2'},inplace=True)
        df2=df.set_index(['Distributor Code']).stack().reset_index().rename(columns={'level_1':                                                                                      'Structure',0:'Shipments'})
        
        df3=df_raw.set_index(['Distributor Code','Day']).stack().reset_index().rename(columns={'level_2':                                                                                                'Structure',0:'Shipments'})
        df_all=pd.concat([df3, df2], ignore_index=True,sort=False)
        df_all['Day']=df_all['Day'].replace([1,2,3,4,5,6,7,None],                                             ['1.Mon','2.Tue','3.Wed','4.Thu','5.Fri','6.Sat','7.Sun',"Total"])
        df_all=df_all.sort_values(['Distributor Code','Structure'],ascending=True).reset_index(drop=True)
        df_all['Update Time']=time.strftime('%m/%d/%Y', time.gmtime(os.path.getmtime(file_unloading)))
        df_all['Week'],df_all['Year'],df_all['File name']=[week_in_file_unloading,                                                           year_in_file_unloading,file_name_in_file_unloading]
        df_unloading=df_all.reindex(columns=['Distributor Code','Year','Week','Structure','Day','Shipments',                                              'File name','Update Time'])

df_unloading

