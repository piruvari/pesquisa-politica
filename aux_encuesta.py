
# coding: utf-8

# In[ ]:

import psycopg2
import sys
import pprint
import unicodedata
from pygeocoder import Geocoder

 
#Create Datawarehouse
conn_dw = psycopg2.connect(host='localhost', dbname='Encuesta_DW', user='usu_encuesta', password='psw_encuesta')
cursor_dw = conn_dw.cursor()
 
cursor_dw.execute("delete from aux_encuesta ;")    
cursor_dw.execute("select id_formulario, id, fecha, lon,lat from ft_encuesta ;")
rows=list(cursor_dw.fetchall())

#Creo las tablas que corresponden a las preguntas y las deleteo y luego inserto
i=0

while i< (len(rows)):
    fila=list(rows[i])
    lon=fila[3]
    lat=fila[4]
    results = Geocoder.reverse_geocode(lat,lon)
    col_values= str(fila[0])+","+str(fila[1])+",'"+str(fila[2])+"',"+str(lon)+","+str(lat)+",'"+str(results.city)+"','"+str(results.country)+"','"+str(results.address)+"','"+str(results.administrative_area_level_1)+"','"+str(results.administrative_area_level_2)+"','"+str(results.route)+"','"+str(results.neighborhood)+"','"+str(results.locality) +"','"+str(results.political)+"','"+str(results.street_number)+"','"+str(results.postal_code)+"'"
    sql_insert="INSERT INTO aux_encuesta (id_formulario,id,fecha,lon,lat,city,country,address,administrative_area_level_1,administrative_area_level_2,route,neighborhood,locality,political,street_number,postal_code ) VALUES ("+col_values+");"
    cursor_dw.execute(sql_insert)
    i+=1


conn_dw.commit()
cursor_dw.close()
conn_dw.close()


# In[ ]:



