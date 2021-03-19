
# coding: utf-8

# In[16]:



import psycopg2
import sys
import pprint
import unicodedata

def normaliza(cadena):
    from unicodedata import normalize, category
    return ''.join([x for x in normalize('NFD', u) if category(x) == 'Ll'])

def elimina_tildes(s):
    return ''.join((c for c in unicodedata.normalize('NFD', s) if unicodedata.category(c) != 'Mn'))

def limpia_cursor(word):
    word=word.replace("']","")
    word=word.replace("['","")
    word=word.replace("',)]","")
    word=word.replace("[('","")
    return word

def limpia_descripcion(word):
    word=word.lower()   
    word=word.replace(" - "," ")   
    word=word.replace(" ","_")
    word=elimina_tildes(word)
    return word


sql_select_respuesta="respuesta_22, respuesta_23,  respuesta_24 ,  respuesta_25 ,  respuesta_26 ,  respuesta_27 , respuesta_28 ,  respuesta_29 ,  respuesta_30 ,  respuesta_31 , respuesta_32 "
formulario="4"

def Carga_bt(sql_select_respuesta, formulario):
    
    sql_select="select * from formulario_"+formulario+";"
    cursor_stg.execute(sql_select)
    colnames = [desc[0] for desc in cursor_stg.description]
    colnames_nuevos=[]
    #alter agrego las columnas de las preguntas pero colocando el nombre correspondiente a la lk creada
    j=0
    while j< (len(colnames)):
        fila1=colnames[j]
        if 'respuesta' in fila1:
            id_pregunta=fila1[10:]
            sql_select="select distinct desc_pregunta_abrev from formulario where id_pregunta="+id_pregunta
            cursor_stg.execute(sql_select)
            id_lk=str(list(cursor_stg.fetchall()))
            id_lk=limpia_cursor(id_lk)
            id_lk=limpia_descripcion(id_lk)        
            colnames_nuevos.append("id_"+id_lk)
            sql_alter="alter table ft_encuesta add column if not exists id_"+id_lk+" int;"
            cursor_dw.execute(sql_alter)
        j+=1

    #insert con la geografia 
    sql_select="select id_formulario,B.id,dispositivo_id,fecha,hora,lon,lat,cast( id_agee *1000  as varchar)||cast( id_Agem *100 as varchar) ||cast( id_Ageb as varchar) id_ageb,cast(cast( id_agee *1000  as varchar)||cast( id_Agem *100 as varchar) ||cast( id_localidad as varchar)  as bigint) id_localidad,cast(cast( id_agee *100000 as varchar) ||cast( id_distrito_local*10 as varchar)  as bigint) id_distrito_local,cast(cast( id_agee *100000 as varchar) ||cast( id_distrito_federal*10 as varchar)  as bigint) id_distrito_federal,cast(cast( id_agee *1000  as varchar)||cast( id_Agem *100 as varchar) ||cast( id_distrito_federal*10  as varchar) ||cast( id_Seccion  as varchar)as bigint) id_seccion, "+sql_select_respuesta+" from  formulario_"+formulario+" A inner join  ubicacion_encuesta B on  (B.id_formulario= "+formulario+" and B.id_encuesta_Respondida=A.id); "
    cursor_stg.execute(sql_select)
    rows=list(cursor_stg.fetchall())


    col_names_nuevos=limpia_cursor(str(colnames_nuevos))
    col_names_nuevos=col_names_nuevos.replace("'","")
    col_values=[]
    for k in range (0,len(colnames_nuevos)+12):
        col_values.append('%s')
    col_values=str(col_values)
    col_values=limpia_cursor(col_values)
    col_values=col_values.replace("'","")


    sql_insert="INSERT INTO ft_encuesta (id_formulario,id,dispositivo_id,fecha,hora,lon,lat,id_area_geo_basica,id_localidad,id_distrito_local, id_distrito_federal, id_seccion ,"+col_names_nuevos+" ) VALUES ("+col_values+");"


    j=0
    while j< (len(rows)):

        fila1=list(rows[j])
        cursor_dw.execute(sql_insert,fila1)
        conn_dw.commit()
        j+=1

    return 0


def Carga_lk(sql_insert, sql_select, sql_delete):
    cursor_dw.execute(sql_delete)
    cursor_stg.execute(sql_select)
    rows=list(cursor_stg.fetchall())
    j=0
    while j< (len(rows)):
        fila1=list(rows[j])
        cursor_dw.execute(sql_insert,fila1)
        conn_dw.commit()
        j+=1
    return 0


 
#Create Datawarehouse
conn_stg = psycopg2.connect(host='localhost', dbname='Encuesta_Staging', user='usu_encuesta', password='psw_encuesta')
conn_dw = psycopg2.connect(host='localhost', dbname='Encuesta_DW', user='usu_encuesta', password='psw_encuesta')

cursor_stg = conn_stg.cursor()
cursor_dw = conn_dw.cursor()
 
cursor_stg.execute("select distinct desc_pregunta_Abrev from formulario;")
rows=list(cursor_stg.fetchall())

#Creo las tablas que corresponden a las preguntas y las deleteo y luego inserto
i=0

while i< (len(rows)):
    fila=str(list(rows[i]))
    #fila=fila.replace("']","")
    #fila=fila.replace("['","")
    fila=limpia_cursor(fila)
    condicion=fila
    #fila=fila.lower()    
    #fila=fila.replace(" - "," ")   
    #fila=fila.replace(" ","_")
    fila=limpia_descripcion(fila)
    sql="CREATE TABLE IF NOT EXISTS LK_"+fila+" (id_"+fila+" integer,desc_"+fila+" character varying(100));"
    cursor_dw.execute(sql)
    sql_delete="delete from LK_"+fila+ " ;"
    cursor_dw.execute(sql_delete)
    sql_insert="INSERT INTO LK_"+fila+" (id_"+fila+",desc_"+fila+") VALUES (%s, %s);"
    sq_select="select distinct id_respuesta, desc_respuesta from formulario where desc_pregunta_Abrev='"+condicion+"';"
    cursor_stg.execute(sq_select)
    datos=list(cursor_stg.fetchall())
    j=0
    while j< (len(datos)):
        fila1=list(datos[j])
        cursor_dw.execute(sql_insert,fila1)
        conn_dw.commit()
        j+=1
    i+=1

#creo el resto de las tablas
cursor_dw.execute("CREATE TABLE IF NOT EXISTS lk_formulario (id_formulario integer,desc_formulario character varying(100));")
cursor_dw.execute("CREATE TABLE IF NOT EXISTS lk_catalogo (id_catalogo integer,desc_catalogo character varying(100));")
cursor_dw.execute("CREATE TABLE IF NOT EXISTS lk_pregunta_abrev (id_pregunta_abrev integer, desc_pregunta_abrev  character varying(100),id_catalogo integer);")
cursor_dw.execute("CREATE TABLE IF NOT EXISTS lk_pregunta (id_pregunta integer, desc_pregunta text,id_formulario integer,id_catalogo integer,id_pregunta_abrev integer );")

#inserto datos en formulario    
tabela="formulario"
sql_insert="INSERT INTO LK_"+tabela+" (id_"+tabela+",desc_"+tabela+") VALUES (%s, %s);"
sql_select="select distinct id_formulario from formulario;"
sql_delete="delete from lk_"+tabela+" ;"
cursor_dw.execute(sql_delete)
cursor_stg.execute(sql_select)
rows=list(cursor_stg.fetchall())
j=0
while j< (len(rows)):
    fila1=list(rows[j])
    fila1.append(tabela+str(fila1))
    cursor_dw.execute(sql_insert,fila1)
    conn_dw.commit()
    j+=1

#inserto datos en catalog    
tabela="catalogo"
sql_insert="INSERT INTO LK_"+tabela+" (id_"+tabela+",desc_"+tabela+") VALUES (%s, %s);"
sql_select="select  distinct id_catalogo, desc_catalogo from formulario;"
sql_delete="delete from lk_"+tabela+" ;"
cursor_dw.execute(sql_delete)
cursor_stg.execute(sql_select)
rows=list(cursor_stg.fetchall())
j=0
while j< (len(rows)):
    fila1=list(rows[j])
    cursor_dw.execute(sql_insert,fila1)
    conn_dw.commit()
    j+=1
    
#inserto datos en pregunta_abrev    
tabela="pregunta_abrev"
sql_insert="INSERT INTO LK_"+tabela+" (id_"+tabela+",desc_"+tabela+",id_catalogo) VALUES (%s, %s,%s);"
sql_select="select min(id_pregunta) id_pregunta_abrev, desc_pregunta_abrev, id_catalogo from formulario group by desc_pregunta_abrev, id_catalogo order by  min(id_pregunta);"
sql_delete="delete from lk_"+tabela+" ;"
cursor_dw.execute(sql_delete)
cursor_stg.execute(sql_select)
rows=list(cursor_stg.fetchall())
j=0
while j< (len(rows)):
    fila1=list(rows[j])
    cursor_dw.execute(sql_insert,fila1)
    conn_dw.commit()
    j+=1
    
#inserto datos en pregunta
#tabela="pregunta_abrev"
#sql_insert="INSERT INTO LK_"+tabela+" (id_"+tabela+",desc_"+tabela+",id_catalogo) VALUES (%s, %s,%s);"
#sql_select="select min(id_pregunta) id_pregunta_abrev, desc_pregunta_abrev, id_catalogo from formulario group by desc_pregunta_abrev, id_catalogo order by  min(id_pregunta);"
#sql_delete="delete from lk_"+tabela+" ;"
#cursor_dw.execute(sql_delete)
#cursor_stg.execute(sql_select)
#rows=list(cursor_stg.fetchall())
#j=0
#while j< (len(rows)):
 #   fila1=list(rows[j])
#  cursor_dw.execute(sql_insert,fila1)
   # conn_dw.commit()
    #j+=1

#inserto datos en pregunta
tabela="pregunta"
sql_insert="INSERT INTO LK_"+tabela+" (id_"+tabela+",desc_"+tabela+",id_formulario,id_catalogo,id_pregunta_abrev) VALUES (%s, %s,%s,%s,%s);"
sql_select="select distinct  f.id_pregunta, f.desc_pregunta,f.id_formulario, f.id_catalogo, A.id_pregunta_Abrev from formulario f inner join (select   min(id_pregunta) id_pregunta_abrev, desc_pregunta_abrev, id_catalogo from formulario group by desc_pregunta_abrev, id_catalogo) A on (f.desc_pregunta_abrev=A.desc_pregunta_abrev);"
sql_delete="delete from lk_"+tabela+" ;"
cursor_dw.execute(sql_delete)
cursor_stg.execute(sql_select)
rows=list(cursor_stg.fetchall())
j=0
while j< (len(rows)):
    fila1=list(rows[j])
    cursor_dw.execute(sql_insert,fila1)
    conn_dw.commit()
    j+=1

# geografia  
tabela=  "area_geo_estatal" 
sql_insert="INSERT INTO LK_"+tabela+" (id_"+tabela+",desc_"+tabela+") VALUES (%s,%s);"
sql_select="select * from (select id_agee * 1000 id  , nombre from agee union select 0 id, 'No determinado' nombre) A order by id;"
sql_delete="delete from LK_"+tabela+" ;"
Carga_lk(sql_insert, sql_select, sql_delete)   

tabela=  "area_geo_municipal"
sql_insert="INSERT INTO LK_"+tabela+" (id_"+tabela+",desc_"+tabela+",id_area_geo_estatal) VALUES (%s,%s,%s);"
sql_select="select * from (select cast(cast( id_agee  * 1000 as varchar)||cast( id_Agem *100 as varchar) as bigint ) id , nombre, id_Agee*1000 id_Agee from agem union select 0 id, 'No determinado' nombre,0 id_Agee ) A order by id"
sql_delete="delete from LK_"+tabela+" ;"
Carga_lk(sql_insert, sql_select, sql_delete)  

tabela=  "area_geo_basica"
sql_insert="INSERT INTO LK_"+tabela+" (id_"+tabela+",desc_"+tabela+",id_area_geo_municipal) VALUES (%s,%s,%s);"
sql_select= "select * from (select cast( id_agee *1000  as varchar)||cast( id_Agem *100 as varchar) ||cast( id_Ageb as varchar) id, id_Ageb nombre, cast(cast( id_agee *1000  as varchar)||cast( id_Agem *100 as varchar) as bigint) id_Agem from ageb union select '0' id, 'No determinado' nombre, 0 id_Agem) A order by id;"
sql_delete="delete from LK_"+tabela+" ;"
Carga_lk(sql_insert, sql_select, sql_delete)

tabela=  "distrito_federal"
sql_insert="INSERT INTO LK_"+tabela+" (id_"+tabela+",desc_"+tabela+",id_area_geo_estatal,id_area_geo_municipal) VALUES (%s,%s,%s,%s);"
sql_select= "select * from (select  cast(cast( id_agee *100000 as varchar) ||cast( id_distrito*10 as varchar)  as bigint) id, cast(id_distrito as varchar) nombre, id_agee *1000  id_Agem, 0 id_agem from distrito_federal union select  distinct cast(cast( id_agee *1000  as varchar)||cast( id_Agem *100 as varchar) ||cast( id_distrito*10  as varchar) as bigint) id, cast(id_distrito as varchar) nombre,  id_agee *1000  id_Agee, cast(cast( id_agee *1000  as varchar)||cast( id_Agem *100 as varchar) as bigint) id_Agem   from seccion union select 0 id, 'No determinado' nombre, 0 id_agee, 0 id_Agem ) A order by  id ;"
sql_delete="delete from LK_"+tabela+" ;"
Carga_lk(sql_insert, sql_select, sql_delete) 

tabela=  "seccion"
sql_insert="INSERT INTO LK_"+tabela+" (id_"+tabela+",desc_"+tabela+",id_distrito) VALUES (%s,%s,%s);"
sql_select= "select * from (select cast(cast( id_agee *1000  as varchar)||cast( id_Agem *100 as varchar) ||cast( id_distrito*10  as varchar) ||cast( id_Seccion  as varchar)as bigint)  id, cast(id_seccion as varchar) nombre, cast(cast( id_agee *1000  as varchar)||cast( id_Agem *100 as varchar) ||cast( id_distrito*10  as varchar) as bigint) id_distrito from seccion  union select 0 id, 'No determinado' nombre, 0 id_distrito) A order by id;"
sql_delete="delete from LK_"+tabela+" ;"
Carga_lk(sql_insert, sql_select, sql_delete) 

tabela=  "localidad"
sql_insert="INSERT INTO LK_"+tabela+" (id_"+tabela+",desc_"+tabela+",id_area_geo_municipal) VALUES (%s,%s,%s);"
sql_select= "select * from (select cast(cast( id_agee *1000  as varchar)||cast( id_Agem *100 as varchar) ||cast( id_localidad as varchar)  as bigint)id,  nombre, cast(cast( id_agee *1000  as varchar)||cast( id_Agem *100 as varchar) as bigint) id_Agem from localidad union select '0' id, 'No determinado' nombre, 0 id_Agem) A order by id;"
sql_delete="delete from LK_"+tabela+" ;"
Carga_lk(sql_insert, sql_select, sql_delete) 

tabela=  "distrito_local"
sql_insert="INSERT INTO LK_"+tabela+" (id_"+tabela+",desc_"+tabela+",id_area_geo_estatal) VALUES (%s,%s,%s);"
sql_select= "select  * from (select  cast(cast( id_agee *100000 as varchar) ||cast( id_distrito*10 as varchar)  as bigint) id, cast(id_distrito as varchar) nombre, id_agee *1000  id_Agee from distrito_local union select 0 id, 'No determinado' nombre, 0 id_agee ) A order by  id ;"
sql_delete="delete from LK_"+tabela+" ;"
Carga_lk(sql_insert, sql_select, sql_delete) 
    

#fact tables
cursor_dw.execute("CREATE TABLE IF NOT EXISTS ft_Encuesta (  id_formulario integer,  id integer,  dispositivo_id integer, fecha date,hora time without time zone, lon numeric,  lat numeric,  id_area_geo_basica character varying(20),  id_localidad bigint ,  id_distrito_local bigint ,  id_distrito_federal bigint ,  id_seccion bigint);")
sql_detele="delete from ft_encuesta"
cursor_dw.execute(sql_detele)


sql_select_respuesta="respuesta_22, respuesta_23,  respuesta_24 ,  respuesta_25 ,  respuesta_26 ,  respuesta_27 , respuesta_28 ,  respuesta_29 ,  respuesta_30 ,  respuesta_31 , respuesta_32 "
formulario="4"
Carga_bt(sql_select_respuesta, formulario)

sql_select_respuesta="respuesta_11, respuesta_12,  respuesta_13 ,  respuesta_14 ,  respuesta_15 ,  respuesta_16 , respuesta_17 ,  respuesta_18 ,  respuesta_19 ,  respuesta_20 , respuesta_21 "
formulario="3"
Carga_bt(sql_select_respuesta, formulario)

cursor_stg.close()
conn_stg.commit()
conn_stg.close()

conn_dw.commit()
cursor_dw.close()
conn_dw.close()







# In[ ]:



