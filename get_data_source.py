import pandas as pd
import numpy as np
import requests
import zipfile
import io
import time
import os
import xml.etree.ElementTree as ET
import plotly.graph_objects as go

base_path = "C:/Users/coold/Documents/NASA Challenge/2020-epi-indicators-time-series"

def get_climate_data(folder):
    
    files = os.listdir(f"{base_path}/{folder}")
    
    df_full = pd.DataFrame()
    for file in files:
        
        df = pd.read_csv(f"{base_path}/{file}")
        ind = file.split("_")[0]
        
        cols_rename = {i:i.split(f"{ind}.ind.")[1] for i in df.columns if ind in i}
        df = df.rename(columns=cols_rename)
        df['Indicator Name'] = ind
        
        df = pd.melt(df, id_vars=['code', 'iso', 'country', 'Indicator Name'], 
                            var_name='date', value_name='value')
        
        df = df.rename(columns={'Indicator Name':'indicator'})
        df = df[['country', 'indicator', 'date', 'value']]
    
        df_full = pd.concat([df_full, df])
       
    df_full = df_full.drop_duplicates(subset=['country', 'indicator', 'date'])
    df_full[['country', 'indicator', 'date']] = df_full[['country', 'indicator', 'date']].astype(str)
    df_full[['value']] = df_full[['value']].astype(float)
    
    df_full.to_parquet("C:/Users/coold/Documents/NASA Challenge/climate_data_tsc.parquet", index=False)
    
    return df_full

def get_gender_data(folder):
    
    files = os.listdir(f"{base_path}/{folder}")
    files = [i for i in files if i.split('_')[1] == 'Data.csv']

    df_full = pd.DataFrame()
    for file in files:
        
        df = pd.read_csv(f"{base_path}/{file}")
        df = df.replace({'..':np.nan})
        
        cols_rename = {i:i[0:4] for i in df.columns if 'YR' in i}
        df = df.rename(columns=cols_rename)
        df = pd.melt(df, id_vars=['Series Name', 'Series Code', 'Country Name', 'Country Code'], 
                            var_name='date', value_name='value')
        
        df = df.rename(columns={'Series Name':'indicator', 'Country Name':'country'})
        df = df[['country', 'indicator', 'date', 'value']]

        df_full = pd.concat([df_full, df])

    df_full = df_full.drop_duplicates(subset=['country', 'indicator', 'date'])
    df_full[['country', 'indicator', 'date']] = df_full[['country', 'indicator', 'date']].astype(str)
    df_full[['value']] = df_full[['value']].astype(float)
    
    df_full.to_parquet("C:/Users/coold/Documents/NASA Challenge/gender_data_tsc.parquet", index=False)
    
    return df_full

def merge_data(gnd_df, cmt_df):
    
    df = pd.merge(gnd_df, cmt_df, on=['date', 'country'], how='outer', suffixes=('_gnd', '_cmt'))
    df['comb_indicators'] = df['indicator_gnd'] + ' - ' + df['indicator_cmt']
    df = df.query("comb_indicators.notna()")
    df = df.sort_values(by=['country', 'comb_indicators', 'date'])
    
    return df

def calculate_covariance(group, col_s1, col_s2):
    return group[col_s1].cov(group[col_s2])

def get_lplot(comb_ind, abv):
    
    x = comb_ind['date']
    y1 = comb_ind['value_gnd']
    y2 = comb_ind['value_cmt']  
    name_y1 = comb_ind['indicator_gnd'].values[0]
    name_y2 = comb_ind['indicator_cmt'].values[0]
    
    fig = go.Figure()
    
    fig.add_trace(go.Scatter(x=x, y=y1, mode='lines', name=name_y1, line=dict(color='purple'), yaxis='y1'))
    fig.add_trace(go.Scatter(x=x, y=y2, mode='markers', name=name_y2, line=dict(color='pink'), yaxis='y2'))

    fig.update_layout(
        title='Relationship in Colombia',
        xaxis=dict(title='Date'),
        yaxis=dict(title=name_y1, titlefont=dict(color='purple'), tickfont=dict(color='purple')),
        yaxis2=dict(title=f"{name_y2} ({abv})", titlefont=dict(color='pink'), tickfont=dict(color='pink'),
                    anchor='x', overlaying='y', side='right'),
        plot_bgcolor='white',  # Fondo del área de trazado en blanco
        paper_bgcolor='white'  # Fondo del gráfico completo en blanco
    )
    
    fig.write_html(f"C:/Users/coold/Documents/NASA Challenge/figures/{name_y1}_{name_y2}.html")
    
    pass

gnd_df = get_climate_data("genderwb")
cmt_df = get_climate_data("2020-epi-indicators-time-series")

df = merge_data(gnd_df, cmt_df)
  
covariance_per_group = df.groupby(['comb_indicators', 'country']).apply(lambda group: calculate_covariance(group, 'value_gnd', 'value_cmt')).reset_index().rename(columns={0:'cov'})

ind = 'Labor force, female - BHV'
comb_ind = df.query("comb_indicators == @ind & country == 'Colombia'")
comb_ind  = comb_ind.sort_values(by=['country', 'comb_indicators', 'date'])
comb_ind['date'] = comb_ind['date'].astype(float)
comb_ind = comb_ind.query("date >= 2008")

get_lplot(comb_ind, 'Biodiversity Hotspots Vulnerability')

ind = 'Labor force, female - FGA'
comb_ind = df.query("comb_indicators == @ind & country == 'Colombia'")
comb_ind  = comb_ind.sort_values(by=['country', 'comb_indicators', 'date'])
comb_ind['date'] = comb_ind['date'].astype(float)
comb_ind = comb_ind.query("date >= 2008")

get_lplot(comb_ind, 'Forest Growth Area')
