import pandas as pd
import numpy as np
def crear_visit_id(data):
  data['visitid'] = data['visitStartTime']
  return data
def crear_flag_transaction(data):
  data['flag_transaction'] = np.where(data['transactionid'].isna(),0,1)
  return data
def generar_primera_agregacion(data):
  df_agrupado = data.groupby(['date','fullvisitorId','visitid']).agg(
      {'bounces':'max',
      'pageviews':'max',
      'medium':'max',
      'deviceCategory':'max',
      'flag_transaction':'max',
      'transactionrevenue_1':'max'}
  ).reset_index()
  return df_agrupado

def generar_columnas(data, df, ecom_type, nom_colum):
  df_filtered = data[data['action_type']==ecom_type]
  df_agg = df_filtered.groupby(['date','fullvisitorId','visitid'])['pagepath'].count().reset_index()
  df_agg = df_agg.rename(columns = {'pagepath':nom_colum})
  df = df.merge(df_agg, how = 'left', on = ['date','fullvisitorId','visitid'])
  df[nom_colum] = df[nom_colum].fillna(0)
  return df

def generar_columns_trx(data, df, ecom_type):
  data_trx = data[data['action_type']==ecom_type]
  data_trx_agg = data_trx.groupby(['date','fullvisitorId','visitid']).agg(
      {
          'transactionid':'nunique',
        'productquantity':'sum'
      }
      ).reset_index()

  data_trx_agg = data_trx_agg.rename(columns = {'transactionid':'numero_trx','productquantity':'total_unidades_compradas'})
  df = df.merge(data_trx_agg, how = 'left', on = ['date','fullvisitorId','visitid'])
  return df

def preprocesamiento(data):
  data = crear_visit_id(data)
  data = crear_flag_transaction(data)
  df = generar_primera_agregacion(data)
  print('Fin de la primera agregación')
  df = generar_columnas(data,df,2,'numero_product_view')
  df = generar_columnas(data,df,3,'numero_add_to_cart')
  df = generar_columnas(data,df,4,'numero_remove_from_cart')
  df = generar_columnas(data,df,5,'numero_checkout')
  print('Fin generación de columns')
  df = generar_columns_trx(data,df,6)
  print('Fin columnas transacción')
  return df