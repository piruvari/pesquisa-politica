
# coding: utf-8

# In[28]:

#
import pymysql
import psycopg2
 
### download de los datos fontes
con=pymysql.connect(host=XXXXX, port =XXX,user='XXX', passwd='XXX',  db='XXX')
con1=pymysql.connect(host='XXXXXX', port =XX,user='findit', passwd='XXX',  db='XXXX') 

cursor=con.cursor()
cursor1=con1.cursor()

cursor.execute("SELECT formulario_id, p.id, p.descripcion, p.catalogo_id, c.nombre, oxc.id,oxc.descripcion FROM `pregunta` p, `opcion_x_catalogo` oxc, `catalogo` c WHERE formulario_id in ( 4,3) AND p.catalogo_id = oxc.catalogo_id and p.catalogo_id = c.id")
rows=list(cursor.fetchall())
cursor.execute("SELECT * FROM `formulario_4`")
rows2=list(cursor.fetchall())
cursor.execute("SELECT * FROM `formulario_3`")
rows3=list(cursor.fetchall())
cursor.execute("SELECT * FROM `ubicacion_encuesta` ")
rows4=list(cursor.fetchall())

cursor1.execute("SELECT id_agee,id_agem,id_ageb,latmin,latmax,longmin,longmax FROM `ageb`")
rowsgeo2=list(cursor1.fetchall())

cursor1.execute("SELECT id_agee,nombre,latmin,latmax,longmin,longmax FROM `agee`")
rowsgeo3=list(cursor1.fetchall())

cursor1.execute("SELECT  id_agee,id_agem,nombre,latmin,latmax,longmin,longmax FROM `agem`")
rowsgeo4=list(cursor1.fetchall())

cursor1.execute("SELECT id_agee,id_distrito,tipo,latmin,latmax,longmin,longmax FROM `distrito_federal`")
rowsgeo5=list(cursor1.fetchall())

cursor1.execute("SELECT  id_agee,id_distrito,tipo,latmin,latmax,longmin,longmax  FROM `distrito_local`")
rowsgeo6=list(cursor1.fetchall())

cursor1.execute("SELECT id_agee,id_agem,id_localidad,nombre,latmin,latmax,longmin,longmax FROM `localidad`")
rowsgeo7=list(cursor1.fetchall())

cursor1.execute("SELECT id_agee,id_agem,id_distrito,id_seccion,tipo,latmin,latmax,longmin,longmax FROM `seccion`")
rowsgeo8=list(cursor1.fetchall())
 
 
## insert de data of mysql to postgresql
 
conn_stg = psycopg2.connect(host='localhost', dbname='Encuesta_Staging', user='usu_encuesta', password='psw_encuesta')
cursor_stg = conn_stg.cursor()

cursor_stg.execute("delete from formulario;")
cursor_stg.execute("delete from formulario_4;")
cursor_stg.execute("delete from formulario_3;") 
cursor_stg.execute("delete from ubicacion_encuesta;")
cursor_stg.execute("delete from ageb;")
cursor_stg.execute("delete from agee;") 
cursor_stg.execute("delete from agem;")
cursor_stg.execute("delete from distrito_federal;")
cursor_stg.execute("delete from distrito_local;") 
cursor_stg.execute("delete from localidad;")
cursor_stg.execute("delete from seccion;") 

i=0
while i< len(rows):
    fila=list(rows[i])
    sql="insert into formulario values ("+str(fila[0])+","+str(fila[1])+",'"+str(fila[2])+"',"+str(fila[3])+",'"+str(fila[4])+"',"+str(fila[5])+",'"+str(fila[6])+"');"
    cursor_stg.execute(sql)
    i+=1
 
sql_update="update formulario set desc_pregunta_abrev = desc_catalogo;update formulario set desc_pregunta_abrev = 'Tiene intesion de votar' where id_pregunta in (16,27);update formulario set desc_pregunta_abrev = 'Posibilidad de cambio de voto' where id_pregunta in (29);update formulario set desc_pregunta_abrev = 'Posibilidad de que voten al mismo candidato' where id_pregunta in (32);update formulario set desc_pregunta_abrev = 'hijos' where id_pregunta in (13,24);update formulario set desc_pregunta_abrev = 'Candidatos presidenciales menos considerados'where id_pregunta in (30);update formulario set desc_pregunta_abrev = 'Candidatos presidenciales mas considerados' where id_pregunta in (28);update formulario set desc_pregunta_abrev = 'Principal problema localidad'where id_pregunta in (17);update formulario set desc_pregunta_abrev = 'Segundo principal problema Localidad' where id_pregunta in (18);update formulario set desc_pregunta_abrev = 'Principal problema de su Municipio'where id_pregunta in (19);update formulario set desc_pregunta_abrev = 'Principal problema Estado'where id_pregunta in (20);update formulario set desc_pregunta_abrev = 'Principal problema Pais'where id_pregunta in (21);"
 
i=0
while i< len(rows2):
    fila=list(rows2[i])
    sql="insert into formulario_4 values ("+str(fila[0])+","+str(fila[1])+",'"+str(fila[2])+"','"+str(fila[3])+"',"+str(fila[4])+","+str(fila[5])+",'"+str(fila[6])+"','"+str(fila[7])+"','"+str(fila[8])+"','"+str(fila[9])+"','"+str(fila[10])+"','"+str(fila[11])+"','"+str(fila[12])+"','"+str(fila[13])+"','"+str(fila[14])+"','"+str(fila[15])+"','"+str(fila[16])+"');"
    cursor_stg.execute(sql)
    i+=1
 
i=0
while i< len(rows3):
    fila=list(rows3[i])
    sql="insert into formulario_3 values ("+str(fila[0])+","+str(fila[1])+",'"+str(fila[2])+"','"+str(fila[3])+"',"+str(fila[4])+","+str(fila[5])+",'"+str(fila[6])+"','"+str(fila[7])+"','"+str(fila[8])+"','"+str(fila[9])+"','"+str(fila[10])+"','"+str(fila[11])+"','"+str(fila[12])+"','"+str(fila[13])+"','"+str(fila[14])+"','"+str(fila[15])+"','"+str(fila[16])+"');"
    cursor_stg.execute(sql)
    i+=1
    
i=0
while i< len(rows4):
    fila=list(rows4[i])
    sql="insert into ubicacion_encuesta values ("+str(fila[0])+","+str(fila[1])+","+str(fila[2])+","+str(fila[3])+","+str(fila[4])+",'"+str(fila[5])+"',"+str(fila[6])+","+str(fila[7])+","+str(fila[8])+","+str(fila[9])+",'"+str(fila[10])+"','"+str(fila[11])+"','"+str(fila[12])+"','"+str(fila[13])+"','"+str(fila[14])+"','"+str(fila[15])+"');"
    cursor_stg.execute(sql)
    i+=1
 
i=0
while i< len(rowsgeo2):
    fila=list(rowsgeo2[i])
    sql="insert into ageb values ("+str(fila[0])+","+str(fila[1])+",'"+str(fila[2])+"',"+str(fila[3])+","+str(fila[4])+","+str(fila[5])+","+str(fila[6])+");"
    cursor_stg.execute(sql)
    i+=1 
    
i=0
while i< len(rowsgeo3):
    fila=list(rowsgeo3[i])
    sql="insert into agee values ("+str(fila[0])+",'"+str(fila[1])+"',"+str(fila[2])+","+str(fila[3])+","+str(fila[4])+","+str(fila[5])+");"
    cursor_stg.execute(sql)
    i+=1    
 
i=0
while i< len(rowsgeo4):
    fila=list(rowsgeo4[i])
    sql="insert into agem values ("+str(fila[0])+","+str(fila[1])+",'"+str(fila[2])+"',"+str(fila[3])+","+str(fila[4])+","+str(fila[5])+","+str(fila[6])+");"
    cursor_stg.execute(sql)
    i+=1 
i=0
while i< len(rowsgeo5):
    fila=list(rowsgeo5[i])
    sql="insert into distrito_federal values ("+str(fila[0])+","+str(fila[1])+","+str(fila[2])+","+str(fila[3])+","+str(fila[4])+","+str(fila[5])+","+str(fila[6])+");"
    cursor_stg.execute(sql)
    i+=1 
    
i=0
while i< len(rowsgeo6):
    fila=list(rowsgeo6[i])
    sql="insert into distrito_local values ("+str(fila[0])+","+str(fila[1])+","+str(fila[2])+","+str(fila[3])+","+str(fila[4])+","+str(fila[5])+","+str(fila[6])+");"
    cursor_stg.execute(sql)
    i+=1 
    
 
i=0
while i< len(rowsgeo7):
    fila=list(rowsgeo7[i])
    remplazocomilla=str(fila[3])
    remplazocomilla=remplazocomilla.replace("'","")
    sql="insert into localidad values ("+str(fila[0])+","+str(fila[1])+","+str(fila[2])+",'"+remplazocomilla+"',"+str(fila[4])+","+str(fila[5])+","+str(fila[6])+","+str(fila[7])+");"
    cursor_stg.execute(sql)
    i+=1 

i=0
while i< len(rowsgeo8):
    fila=list(rowsgeo8[i])
    sql="insert into seccion values ("+str(fila[0])+","+str(fila[1])+","+str(fila[2])+","+str(fila[3])+","+str(fila[4])+","+str(fila[5])+","+str(fila[6])+","+str(fila[7])+","+str(fila[8])+");"
    cursor_stg.execute(sql)
    i+=1 
 
#cierro conexiones
cursor.close()
con.close()
cursor1.close()
con1.close()
cursor_stg.execute(sql_update)
cursor_stg.close
conn_stg.commit()
conn_stg.close()


# In[ ]:



