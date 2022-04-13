# -*- coding: utf-8 -*-
"""
Created on Tue Apr 12 12:44:15 2022

@author: AbdurRahman
"""

import pandas as pd
import csv

beijing_df1 = pd.read_csv('beijing_17_18_aq.csv')
beijing_df1.drop(columns = ['PM10', 'NO2', 'CO', 'O3', 'SO2'], inplace = True)

beijing_df1['utc_time'] = pd.to_datetime(beijing_df1.utc_time)
beijing_df1.set_index(['utc_time','stationId'], inplace = True, drop = True)

beijing_df2 = beijing_df1.pivot_table(values='PM2.5', index=['utc_time'], columns='stationId')

station_locs = pd.read_csv('beijing_station_locations.csv')

beijing_df2.reset_index(inplace = True)


#%%

def aggregate(df):
    time_dict = {}
    for ind in df.index:
        if not df['time'][ind] in time_dict.keys():
            time_dict[df['time'][ind]] = {'conc_agg': df['conc'][ind], 'count': 1}
        else:
            time_dict[df['time'][ind]] = {'conc_agg': time_dict[df['time'][ind]]['conc_agg'] + df['conc'][ind], 'count': time_dict[df['time'][ind]]['count'] + 1}
    df_ret = pd.DataFrame.from_dict(time_dict, orient = 'index')
    return df_ret.reset_index()

def hourly_avg(main_df):
    us_df_h = main_df.copy()
    us_df_h['time'] = us_df_h['time'].dt.strftime("%Y-%m-%d")
    us_df_h['time'] = pd.to_datetime(us_df_h['time'])
    #renke_df2 = renke_df2.groupby('time').mean().reset_index()
    
    us_df_h2 = aggregate(us_df_h)
    us_df_h2 = us_df_h2.where(us_df_h2['count']>=18)
    

    us_df_h2['conc'] = us_df_h2['conc_agg']/us_df_h2['count']
    us_df_h2.dropna(subset = ['conc'], inplace = True)
    us_df_h2.rename(columns = {'index': 'time'}, inplace = True)
    us_df_h2.drop(columns = ['conc_agg', 'count'], inplace = True)    

    
    us_df_h2['location'] = us_df_h['location'].where(us_df_h2['time'].isin(us_df_h['time']))
    us_df_h2['lat'] = us_df_h['lat'].where(us_df_h2['time'].isin(us_df_h['time']))
    us_df_h2['long'] = us_df_h['long'].where(us_df_h2['time'].isin(us_df_h['time']))
    return us_df_h2

#%%

df_list = []

for column in beijing_df2.columns[1:36]:
    df = pd.DataFrame()
    df['time'] = pd.to_datetime(beijing_df2['utc_time'])
    df['conc'] = beijing_df2[column].tolist()
    df['location'] = column
    df['lat'] = float(station_locs[station_locs['ID'] == column]['latitude'])
    df['long'] = float(station_locs[station_locs['ID'] == column]['longitude'])
    df_list.append(df.copy())


#%%

df_list2 = []

for df in df_list:
    df_list2.append(hourly_avg(df))


















