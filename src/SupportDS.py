import pandas as pd
import os
from datetime import date

producto = 'ACEITE'
presentacion = 'BOTELLA 946 ML. SOYA'
marca = 'NUTRIOLI'
municipio = 'GUSTAVO A. MADERO'
marca_tienda = 'WAL-MART'
nombre_tienda = 'WALMART SUCURSAL TEPEYAC'

anio_inicial = 2015
anio_final = 2016 # No inclusivo
semana_inicial = 1
semana_final = 53  # No Inclusivo

df_concat = pd.DataFrame(columns=['fecha','precio','diferencia_minimo'])

# 1. Obtener datos existentes para los años y semanas indicados
for y in range(anio_inicial, anio_final):
    for i in range(semana_inicial,semana_final):
        file_name = 'C:/Users/Guadalupe/Downloads/QQP_'+str(y)+'/'+str(y)+'/'+f"{i:02d}"+str(y)+'.csv'
        print(file_name)
        if os.path.exists(f'{file_name}'):
            df_raw = pd.read_csv(file_name, names=['producto','presentacion','marca','categoria','grupo','precio','fecha','marca_tienda','tipo_tienda','nombre_tienda','direccion','estado','municipio','latitud','longitud'], index_col=False)
            df_mins = df_raw.groupby(['producto','presentacion','marca'])['precio'].min()
            df_group_min = df_raw.merge(df_mins, on=['producto','presentacion','marca'], how='inner')
            df_group_min['diferencia_minimo'] = df_group_min['precio_x']/df_group_min['precio_y']
            df_group_min.rename(columns={'precio_x': 'precio'}, inplace=True, errors='raise')
            df_raw = df_group_min
            filter1 = df_raw['producto'] == producto
            filter2 = df_raw['presentacion'] == presentacion
            filter3 = df_raw['marca'] == marca
            filter4 = df_raw['marca_tienda'] == marca_tienda
            filter5 = df_raw['municipio'] == municipio
            #filter6 = df_raw['nombre_tienda'] == nombre_tienda
            df = df_raw[filter1 & filter2 & filter3 & filter4 & filter5]
            if df.shape[0] > 0:
                df_concat = pd.concat([df_concat,df[['fecha','precio','diferencia_minimo']]])
                prev_precio = df['precio'].mean()
                prev_diferencia_minimo = df['diferencia_minimo'].mean()

# 2. Determinar año y semana de cada dato existente
df_concat['fechad'] = pd.to_datetime(df_concat['fecha'])
df_concat["anio"] = df_concat["fechad"].dt.isocalendar().year
df_concat["semana"] = df_concat["fechad"].dt.isocalendar().week

# 3. Interpolar semanas faltantes con los datos previos más cercanos
prev_precio = 0
prev_diferencia_minimo = 0

for y in range(anio_inicial, anio_final):
     for i in range(semana_inicial,semana_final):
         date_week = date.fromisocalendar(y, i, 5)
         filtro_annio = df_concat['anio']==y
         filtro_semana = df_concat['semana']==i
         df_semana = df_concat[filtro_annio & filtro_semana]
         if df_semana.shape[0]>0:
             prev_precio = df_semana['precio'].iloc[0]
             prev_diferencia_minimo = df_semana['diferencia_minimo'].iloc[0]
         else:
             date_week = date.fromisocalendar(y, i, 5)
             row = {'fecha':[str(date_week)], 'precio':[prev_precio], 'diferencia_minimo':[prev_diferencia_minimo]}
             df_concat = pd.concat([df_concat,pd.DataFrame(row)])

# 4. Preparar dataset para salvar archivo
df_casi = df_concat[['fecha','precio','diferencia_minimo']]
df_casi.set_index('fecha', inplace=True)
df_final = df_casi.sort_index(ascending=True)
print(df_final)

# 5. Salvar dataset en archivo
df_final.to_csv('C:/Users/Guadalupe/Downloads/QQP_'+str(anio_inicial)+'_'+str(anio_final)+'-'
                +producto+'_'+presentacion+'_'+marca+'_'+marca_tienda+'_'+municipio+'.csv')