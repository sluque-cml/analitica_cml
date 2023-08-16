import pandas as pd
import numpy as np
import seaborn as sb
import smtplib
from email.message import EmailMessage

#leer archivo fuente
df_trafico = pd.read_csv("Z:\\2023\\08 AGOSTO\\14 LUNES\ANALISIS_TRAFICO\\trafico.csv")

#renombrar columnas
df_trafico=df_trafico.rename(columns={"Descripci__n_del_evento":"descripcion", "Nombre_de_recurso":"nombre","Usuario":"usuario","Fecha":"fecha","Nombre_del_evento":"evento","ID_de_recurso":"id_recurso","Tipo_de_recurso":"tipo","Propietario":"propietario","Direcci__n_IP":"ip","Tipo_de_conector":"conector","Visibilidad_anterior":"visibilidad_anterior","Visibilidad":"visibilidad","DIAMES":"diames","DIA":"dia","MES":"mes","NUMEROMES":"numero_mes","A__O":"anio","FECHATEXTO":"fecha_texto","AREA":"area"})

#ajustar tipos de datos
#campo fecha: 30 ago. 2021 11:47:14 GMT-5
#extraer el dia:
df_trafico["fecha_dia"]=df_trafico["fecha"].str[:2]




df_trafico = df_trafico[df_trafico['fecha_dia'].notna()]

df_trafico['fecha'] = pd.to_datetime(df_trafico['fecha'])


#leer archivo de transacciones (el archivo viene sin nombres de columnas)
df_trx = pd.read_csv("Z:\\2023\\TRANSACCIONES12.txt", sep="|", encoding='ISO-8859-1', header=None, names=['codigo','talla','fecha','cod_tienda','ven_und','ven_val','inv','costo_ven','multrot','ticket','vende','desc_ven','cedula','nombres','iva','origen','suborigen','valor_iva','venta_sin_iva','nomvende','cargo','compania'] )

#limpieza de datos
df_trx_filter = df_trx.fillna(0) #colocar 0 en los campos NaN y no elimina las filas

#ajuste de tipos de datos
df_trx['fecha'] = pd.to_datetime(df_trx['fecha'])
df_trx['codigo'] = df_trx['codigo'].astype('string')
df_trx['cod_tienda'] = df_trx['cod_tienda'].astype('string')
df_trx['vende'] = df_trx['vende'].astype('string')
df_trx['compania'] = df_trx['compania'].astype('string')
df_trx['cargo'] = df_trx['cargo'].astype('string')
df_trx['nomvende'] = df_trx['nomvende'].astype('string')
df_trx['origen'] = df_trx['origen'].astype('string')
df_trx['suborigen'] = df_trx['suborigen'].astype('string')
df_trx['cedula'] = df_trx['cedula'].astype('string')
df_trx['nombres'] = df_trx['nombres'].astype('string')
df_trx['ticket'] = df_trx['ticket'].astype('string')
df_trx['talla'] = df_trx['talla'].astype('string')

#agrupar
df_trx_resumen = df_trx.groupby(['fecha']).sum()
df_trx_filter.groupby(['codigo']).sum() #agrupar por una columna
df_trx_filter.groupby(['codigo','cod_tienda']).sum() #agrupar por dos columnas
#tomar solo una tienda del dataframe filtrado
df_acum = df_trx_filter.groupby(['cod_tienda','fecha']).sum().groupby(level=[0]).cumsum() #bajar los datos agrupados a otro dataframe

"""
AJUSTE DE TIPOS DE DATOS
"""
#ajuste de tipos de datos del dataframe filtrado
df_trx_filter['fecha'] = pd.to_datetime(df_trx_filter['fecha'])
df_trx_filter['codigo'] = df_trx_filter['codigo'].astype('string')
df_trx_filter['cod_tienda'] = df_trx_filter['cod_tienda'].astype('string')
df_trx_filter['vende'] = df_trx_filter['vende'].astype('string')
df_trx_filter['compania'] = df_trx_filter['compania'].astype('string')
df_trx_filter['cargo'] = df_trx_filter['cargo'].astype('string')
df_trx_filter['nomvende'] = df_trx_filter['nomvende'].astype('string')
df_trx_filter['origen'] = df_trx_filter['origen'].astype('string')
df_trx_filter['suborigen'] = df_trx_filter['suborigen'].astype('string')
df_trx_filter['cedula'] = df_trx_filter['cedula'].astype('string')
df_trx_filter['nombres'] = df_trx_filter['nombres'].astype('string')
df_trx_filter['ticket'] = df_trx_filter['ticket'].astype('string')
df_trx_filter['talla'] = df_trx_filter['talla'].astype('string')

"""
SUMAS
"""
#suma de una columna
df_trx_filter['ven_und'].sum()  #suma los datos de una columna
df_trx_filter[['ven_und','venta_sin_iva']].sum() #suma varias columnas
df_trx_filter.groupby(['cod_tienda','fecha']).sum().groupby(level=[0]).cumsum() #suma acumulativa por tienda y por fecha
df_trx_filter[df_trx_filter.groupby(['cod_tienda','fecha']).sum().groupby(level=[0]).cumsum()]

"""
ORDENAMIENTOS
"""
#ordenar por un campo
df_trx_filter.sort_values(by='ven_und') #por defecto ordena de menor a mayor por ven_und
df_trx_filter.sort_values(by='ven_und', ascending=False) #ordenar de mayor a menor por ven_und


"""
FILTRADO DE REGISTROS POR FILA
"""
#filtrado de datos de una columna
df_trx_filter[['codigo', 'ven_und']]
#filtrado por filas
#1. por indices: filtrado de la fila 0
df_trx_filter.iloc[0]               #solo la fila 0
df_trx_filter.iloc[0:3]             #de la fila 0 a la 2
df_trx_filter.iloc[[1,3,5,7]]       #las filas 1, 3, 5 y 7

#2. filtrado por identificadores
df_trx_filter.loc[[248573,248574]]

#3.filtrado de filas y columnas
df_trx_filter.loc[[248573,248574],['inv','ven_und']] #toma de las filas 248573 y 248574 las columnas inv y ven_und solamente

#filtrado por condiciones
df_trx_filter[df_trx_filter['ven_und'] > 4]                                             #tomar las filas que tengan venta und >4
df_trx_filter[(df_trx_filter['ven_und'] > 4) & (df_trx_filter['cod_tienda'] == '205')]  #tomar todas las filas donde ven_und>4 y tienda = 205
df_trx_filter[df_trx_filter['nombres'].str.contains('GARCIA')] #filtrar solo los clientes con apellido garcia

#transformacion de datos
#crear nuevas columnas: formulas, algoritmos, etc
#margen = (venta_sin_iva -(costo_ven - desc_ven))/venta_sin_iva
#definimos la funcion de trasferencia
def calc_margen(fila):
    if fila['venta_sin_iva']>0:
        margen = (fila['venta_sin_iva']-(fila['costo_ven'] - fila['desc_ven']))/fila['venta_sin_iva']
    else:
        margen=0
    return margen
#aplicar la funcion
df_trx_filter['margen'] = df_trx_filter.apply(calc_margen, axis=1 )






"""
GENERACION DEL CORREO ELECTRONICO DE RESUMEN
INICIO CORREO
"""
#contraseña de aplicacion gmail biadmin ieuyihhnvmkcswkk
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from smtplib import SMTP
import smtplib
import sys
recipients = ['sluque@cmlogistics.com.co']
emaillist = [elem.strip().split(',') for elem in recipients]
msg = MIMEMultipart()
msg['Subject'] = 'Resumen de procesamiento diario'
msg['From'] = 'biadmin@cmlogistics.com.co'
html ="""\
<html>
<head></head>
<body>
{0}
</body>
</html>
""".format(df_trx_resumen.to_html())
part1 = MIMEText(html, 'html')
msg.attach(part1)
server = smtplib.SMTP_SSL('smtp.gmail.com')
server.login('biadmin@cmlogistics.com.co','ieuyihhnvmkcswkk')
server.sendmail(msg['From'], emaillist, msg.as_string())
server.quit()
"""
FIN CORREO
"""


"""
TRABAJO CON EL DATAFRAME
"""
