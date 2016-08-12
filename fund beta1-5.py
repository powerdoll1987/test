# -*- coding: utf-8 -*-
"""
Created on Tue Jul  5 12:45:41 2016

@author: wei.liu
"""
dgreg
wef

import pandas as pd
import statsmodels.api as sm
import numpy as np
from pandas.tseries.offsets import Day

def exp_power(data):
    
    ratio_tmp = 0
    res = sm.OLS(np.asarray(data.ix[:,'SPX Index']),np.array(data.ix[:,'ALL'])).fit()
    ratio_tmp = res.params[0]
    return ratio_tmp
    
def z_score_f(x):
    zscore = (x - x.mean())/x.std()    
    return float(zscore.ix[-1,:])

 
def rolling(d,day,func,name):
    mat_exp = pd.DataFrame()
    day -= 1    
    start_id = day
    while start_id < len(d.index):
        interval = np.arange(start_id - day,start_id + 1)
        x = d.ix[d.index[interval],:]
        mat_exp.at[d.index[start_id],name] = func(x)
        start_id += 1 
    return mat_exp


def corr_withspx(data):
    m = pd.DataFrame()
    for name in data.columns:
       m.at[name,'Correlation'] = np.corrcoef(data[name],data['SPX Index'])[0][1]
    return m

'''用每周的return计算每周的beta'''
def return_ratio(data):
     ret = pd.DataFrame()
     list_mon = []
     list_fri = []
     for i in data.index:
         if i.weekday() == 0:
             list_mon.append(i)
         if i.weekday() == 4:
             list_fri.append(i) 
     for mon in list_mon:
         fri = mon + 4 * Day()
         if fri in list_fri:
             ret.at[fri,'Beta'] = (data.ix[fri,'ALL'] - data.ix[mon,'ALL']) / (data.ix[fri,'SPX Index'] - data.ix[mon,'SPX Index'])
             ret.at[fri,'SPX'] = data.ix[fri,'SPX Index']
     return ret


def normalization(data):
    data[data==0] = None
    data = data.bfill()
    data = data / data.ix[0,:]
    numOfCol = len(data.columns)
    for i in data.index:
        data.at[i, 'ALL'] = sum(data.ix[i,1:numOfCol])
    return data



data_non_ret = pd.read_excel('fund.xlsx')
data_non_ret.set_index('Date', inplace=True)

raw_data = normalization(data_non_ret)
#raw_data.to_excel('fund2.xlsx')

data = raw_data.copy()
#data = pd.read_excel('fund2.xlsx')
#data.set_index('Date', inplace=True)

m = corr_withspx(data)
m.to_excel("corr.xlsx")

data = data[['ALL','SPX Index']].diff().dropna()
mm = rolling(data,21,exp_power,'Beta')
dd = rolling(mm,100,z_score_f,'Z-score')
cc = pd.concat([mm,dd,data_non_ret['SPX Index'].ix[dd.index],raw_data['ALL'].ix[dd.index]],axis = 1,join = 'inner')
cc.to_excel('fund_beta.xlsx')

#ret = return_ratio(data)
#ret.to_excel('ret_ratio.xlsx')








  