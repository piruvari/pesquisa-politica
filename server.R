#
# This is the server logic of a Shiny web application. You can run the 
# application by clicking 'Run App' above.
#
# Find out more about building applications with Shiny here:
# 
#    http://shiny.rstudio.com/
#

#



library(htmlwidgets)
library(tidyr)
library(shiny)
library(RPostgreSQL)
library(shinydashboard)
library(RColorBrewer)
require(ggplot2)
library(plotrix)
library(leaflet)
library(dplyr)
library(DT)
library(purrr)
library(highcharter)

#conexion
pg = dbDriver("PostgreSQL")
pw<-"postgres"
con = dbConnect(pg, user='postgres', password=pw, host="----", port= ---, dbname="----")
rm(pw)

# query the data from postgreSQL 
campos<-"select desc_formulario,e.id,e.fecha,e.lon ,e.lat ,desc_area_geo_basica, desc_localidad, desc_distrito_local,  desc_distrito_federal, desc_seccion,administrative_area_level_1,administrative_area_level_2, desc_candidatos_presidenciales_mas_considerados, desc_genero,desc_rango_etario,desc_hijos, desc_nivel_de_Estudios,desc_nivel_economico_focos, desc_tiene_intesion_De_votar, desc_posibilidad_De_cambio_de_voto,desc_candidatos_presidenciales_menos_Considerados,desc_concubinos_que_votan, desc_posibilidad_de_que_voten_al_mismo_candidato, desc_nivel_economico_ingreso,desc_principal_problema_localidad,desc_segundo_principal_problema_localidad,desc_principal_problema_de_su_municipio ,desc_principal_problema_estado ,desc_principal_problema_pais ,count(1) cantidad "

tablas<-"from ft_encuesta e inner join lk_formulario z on (e.id_formulario=z.id_formulario) inner join aux_encuesta aux on (e.id_formulario=aux.id_formulario and e.id=aux.id) left outer join lk_area_geo_basica y on (e.id_area_geo_basica=y.id_area_geo_basica) left outer join lk_localidad x on (e.id_localidad=x.id_localidad) left outer join lk_distrito_local w on (e.id_distrito_local= w.id_distrito_local)left outer join lk_distrito_federal v on (e.id_distrito_federal=v.id_distrito_federal) left outer join lk_seccion u on (e.id_seccion=u.id_seccion) left outer join lk_candidatos_presidenciales_mas_considerados i on (e.id_candidatos_presidenciales_mas_considerados=i.id_candidatos_presidenciales_mas_considerados) left outer join lk_genero ge on (e.id_genero=ge.id_genero) left outer join lk_rango_etario RE on (e.id_rango_etario=RE.id_rango_etario) left outer join lk_hijos H on (e.id_hijos=H.id_hijos) left outer join lk_nivel_de_estudios ES on (e.id_nivel_de_estudios=ES.id_nivel_de_estudios) left outer join lk_nivel_economico_focos FO on (e.id_nivel_economico_focos=FO.id_nivel_economico_focos) left outer join lk_tiene_intesion_de_votar IV on (e.id_tiene_intesion_de_votar=IV.id_tiene_intesion_de_votar) left outer join lk_posibilidad_de_cambio_de_voto CV on (e.id_posibilidad_de_cambio_de_voto=CV.id_posibilidad_de_cambio_de_voto) left outer join lk_candidatos_presidenciales_menos_considerados MC on (e.id_candidatos_presidenciales_menos_considerados= MC.id_candidatos_presidenciales_menos_considerados) left outer join lk_concubinos_que_votan CQV on (e.id_concubinos_que_votan=CQV.id_concubinos_que_votan) left outer join lk_posibilidad_de_que_voten_al_mismo_candidato PVC on (e.id_posibilidad_de_que_voten_al_mismo_candidato=PVC.id_posibilidad_de_que_voten_al_mismo_candidato) left outer join lk_nivel_economico_ingreso a on (e.id_nivel_economico_ingreso=a.id_nivel_economico_ingreso) left outer join lk_principal_problema_localidad b on (e.id_principal_problema_localidad=b.id_principal_problema_localidad) left outer join lk_segundo_principal_problema_localidad c on (e.id_segundo_principal_problema_localidad=c.id_segundo_principal_problema_localidad) left outer join lk_principal_problema_de_su_municipio d on (e.id_principal_problema_de_su_municipio=d.id_principal_problema_de_su_municipio)left outer join lk_principal_problema_estado f on (e.id_principal_problema_estado=f.id_principal_problema_estado) left outer join lk_principal_problema_pais g on (e.id_principal_problema_pais=g.id_principal_problema_pais) "

groupby<-"group by desc_formulario,e.id,e.fecha,e.lon ,e.lat ,desc_area_geo_basica, desc_localidad, desc_distrito_local,  desc_distrito_federal, desc_seccion,administrative_area_level_1,administrative_area_level_2,desc_candidatos_presidenciales_mas_considerados, desc_genero,desc_rango_etario, desc_hijos, desc_nivel_de_Estudios,desc_nivel_economico_focos, desc_tiene_intesion_De_votar, desc_posibilidad_De_cambio_de_voto, desc_candidatos_presidenciales_menos_Considerados,desc_concubinos_que_votan, desc_posibilidad_de_que_voten_al_mismo_candidato, desc_nivel_economico_ingreso, desc_principal_problema_localidad, desc_segundo_principal_problema_localidad, desc_principal_problema_de_su_municipio , desc_principal_problema_estado , desc_principal_problema_pais "

orderby<-"order by desc_formulario,e.id,e.fecha,e.lon ,e.lat ,desc_area_geo_basica, desc_localidad, desc_distrito_local,  desc_distrito_federal, desc_seccion,administrative_area_level_1,administrative_area_level_2, desc_candidatos_presidenciales_mas_considerados, desc_genero,desc_rango_etario, desc_hijos, desc_nivel_de_Estudios,desc_nivel_economico_focos, desc_tiene_intesion_De_votar,desc_posibilidad_De_cambio_de_voto, desc_candidatos_presidenciales_menos_Considerados,desc_concubinos_que_votan, desc_posibilidad_de_que_voten_al_mismo_candidato, desc_nivel_economico_ingreso, desc_principal_problema_localidad, desc_segundo_principal_problema_localidad, desc_principal_problema_de_su_municipio ,desc_principal_problema_estado , desc_principal_problema_pais "

df_nombrescolumnas<-c("desc_formulario","id","fecha","lon" ,"lat" ,"desc_area_geo_basica", "desc_localidad", "desc_distrito_local",  "desc_distrito_federal", "desc_seccion","administrative_area_level_1","administrative_area_level_2", "desc_candidatos_presidenciales_mas_considerados", "desc_genero","desc_rango_etario","desc_hijos", "desc_nivel_de_estudios","desc_nivel_economico_focos", "desc_tiene_intesion_de_votar", "desc_posibilidad_de_cambio_de_voto","desc_candidatos_presidenciales_menos_considerados","desc_concubinos_que_votan", "desc_posibilidad_de_que_voten_al_mismo_candidato", "desc_nivel_economico_ingreso","desc_principal_problema_localidad","desc_segundo_principal_problema_localidad","desc_principal_problema_de_su_municipio" ,"desc_principal_problema_estado" ,"desc_principal_problema_pais")

# Define server logic required to draw a histogram
shinyServer(function(input, output,session) {
  set.seed(122)

    tablamaster <- eventReactive(input$action, {
  #  pg1 = dbDriver("PostgreSQL")
   # pw1<-"postgres"
    #con1 = dbConnect(pg1, user='postgres', password=pw1, host="localhost", port= 5432, dbname="Encuesta_DW")
  #  rm(pw1)
    filtrodate<-paste(input$dates ,collapse="' and '")
    filtroformulario<-paste(input$Formulario ,collapse=",")
    filtroEstado<-paste(input$e1 ,collapse="', '")
    filtroMunicipio<-paste(input$e2 ,collapse="','")
    where<-paste0(" where e.fecha between '",filtrodate,"' and e.id_formulario in (",filtroformulario,") and administrative_area_level_1 in ('",filtroEstado,"') and administrative_area_level_2 in ('",filtroMunicipio,"') " )
    query<-paste0(campos, tablas,where, groupby, orderby)
    df_rawdata1<-dbGetQuery(con,query)
  #  dbDisconnect(con1)
   # dbUnloadDriver(pg1)
    df_rawdata1
    
  })
  
  
  output$resumen <-DT::renderDataTable({ #renderPrint({ 
    df_tablamaster<-data.frame()
    df_tablamastersummary<-data.frame()
    df_tablamaster<-tablamaster()
    df_tablamastersummary<-df_tablamaster
    df_candidatosmasconsiderados<-aggregate(df_tablamaster$cantidad, list(df_tablamaster$desc_candidatos_presidenciales_mas_considerados),sum)
    names(df_candidatosmasconsiderados)<-c("candidatos", "cantidad")
    
    columnas<-ncol(df_tablamastersummary)
    x <- 1
    
    while(x < columnas ) {
      
      if(class(df_tablamastersummary[[x]])=="character") { 
        df_tablamastersummary[[x]]<-as.factor(df_tablamastersummary[[x]])} 
      x <- x+1
    }
  #  summary(df_tablamastersummary)
    R<-data.frame(summary(df_tablamastersummary))
    R<-R[,2:3]
    names(R)<-c("campo", "Frecuencia/Valor")
    R<-na.omit(R)
    R<-data.frame(R)
    DT::datatable( R,# filter = 'bottom', 
                   options = list(pageLength = 15,# autoWidth = TRUE,
                                  scrollY = '500px', scrollX = TRUE) ) 
    

  })
  
  # Sorting asc
  observeEvent(input$a2z, {
    updateCheckboxGroupInput(
      session = session, inputId = "check2", choices = df_nombrescolumnas, selected = input$check2
    )
  })
  # Sorting desc
  observeEvent(input$z2a, {
    updateCheckboxGroupInput(
      session = session, inputId = "check2", choices = rev(df_nombrescolumnas), selected = input$check2
    )
  })
  #output$res2 <- renderPrint({
 #   input$check2
#  })
  # Select all / Unselect all
  observeEvent(input$all, {
    if (is.null(input$check2)) {
      updateCheckboxGroupInput(
        session = session, inputId = "check2", selected = df_nombrescolumnas
      )
    } else {
      updateCheckboxGroupInput(
        session = session, inputId = "check2", selected = ""
      )
    }
  })
  output$map <- renderLeaflet({
# Create 10 markers (Random points)
    #df_distribucion<-dbGetQuery(con,"select  lon, lat, 1 from ft_encuesta")
    df_distribucion<-data.frame()
    df_distribucion<-tablamaster()
    df_distribucion<-data.frame(df_distribucion$lon, df_distribucion$lat, df_distribucion$cantidad)
    names(df_distribucion)<-c("lon","lat","cantidad")
   # require(leaflet)
    total<-nrow(df_distribucion)
    data=data.frame(long=c(df_distribucion$lon) ,  lat=c(df_distribucion$lat), val=round(rnorm(total),2) , name= paste("point",letters[1:total],sep="_")   ) 
  #  data=data.frame(long=c(-93.12676,-102.28500) ,  lat=c(16.75788,22.41680), val=round(rnorm(2),2) , name= paste("point",letters[1:2],sep="_")   ) 
    
    # Show a marker at each position
   # m=leaflet(data = data) %>% addTiles() %>% addMarkers(~long, ~lat, popup = ~as.character(name))
    #m
    #require(leaflet)
   # data=data.frame(long=c(-93.12676,-102.28500) ,  lat=c(16.75788,22.41680), val=round(rnorm(2),2) , name= paste("point",letters[1:2],sep="_")   ) 
    
    # Show a marker at each position
    m=leaflet(data = data) %>% addTiles() %>% addMarkers(~long, ~lat, popup = ~as.character(name))
    m
  })
  output$Necesidad <- renderLeaflet({
    # Create 20 markers (Random points)
    data=data.frame(long=sample(seq(-150,150),20) ,  lat=sample(seq(-50,50),20) , val=round(rnorm(20),2) , name=paste("point",letters[1:20],sep="_")  ) 
    
    # Show a CUSTOM circle at each position. Size in meters --> change when you zoom.
    m=leaflet(data = data) %>% addTiles()  %>%  addProviderTiles("Esri.WorldImagery") %>%
      addCircles(~long, ~lat, 
                 radius=~val*1000000 , 
                 color=~ifelse(data$val>0 , "red", "orange"),
                 stroke = TRUE, 
                 fillOpacity = 0.2,
                 popup = ~as.character(name)
      ) %>% 
      setView( lng = 166.45, lat = 21, zoom = 2)
    m
  })
  
  output$indecisos <- renderPlot({
  #  df_indecisos<-dbGetQuery(con,"select cv.desc_posibilidad_De_cambio_de_voto, count(1) cantidad from ft_encuesta e inner join lk_posibilidad_de_cambio_de_voto cv on (e.id_posibilidad_de_cambio_de_voto=CV.id_posibilidad_de_cambio_de_voto) group by cv.desc_posibilidad_De_cambio_de_voto;")
    df_indecisos<-data.frame()
    df_indecisos<-tablamaster()
    df_indecisos <-aggregate(df_indecisos$cantidad,by=c(df_indecisos[20]),FUN=sum)
    names(df_indecisos)<-c("desc_posibilidad_De_cambio_de_voto", "cantidad")
    df_indecisos$fraction = round(df_indecisos[[2]] / sum(df_indecisos[[2]] )*100, digits = 2)
    df_indecisos$labels =paste( df_indecisos$fraction,"%", sep=" ")
    df_indecisos$labels <- paste(df_indecisos$labels, "\n", df_indecisos[[1]] , sep="")
    df_indecisos = df_indecisos[order(df_indecisos$fraction), ]
    colores= brewer.pal(4, "BuPu") 
    require(plotrix)
    pie3D(df_indecisos$fraction,labels = df_indecisos$labels,explode = 0.1  ,  main = "% de Indecision", col= colores)

  })

  output$pop_edad <- renderPlot({
    df_PopEdad<-data.frame()
    df_PopEdad<-tablamaster()
  #  df_PopEdad<-dbGetQuery(con,"select desc_candidatos_presidenciales_mas_considerados,RE.desc_rango_etario,count(1) cantidad from ft_encuesta e inner join lk_candidatos_presidenciales_mas_considerados c on (e.id_candidatos_presidenciales_mas_considerados=c.id_candidatos_presidenciales_mas_considerados) inner join lk_rango_etario RE on (e.id_rango_etario=RE.id_rango_etario) group by desc_candidatos_presidenciales_mas_considerados,RE.desc_rango_etario order by desc_candidatos_presidenciales_mas_considerados,RE.desc_rango_etario")
    df_PopEdad <-aggregate(df_PopEdad$cantidad,by=c(df_PopEdad[13],df_PopEdad[15]),FUN=sum)
    names(df_PopEdad)<-c("desc_candidatos_presidenciales_mas_considerados","desc_rango_etario","cantidad")
    # Data
    name= df_PopEdad[[1]]
    average= df_PopEdad[[3]]
    number= df_PopEdad[[3]]
    colores1=c(rgb(0.3,0.1,0.4,0.6) , rgb(0.3,0.5,0.4,0.6) , rgb(0.3,0.9,0.4,0.6) ,  rgb(0.3,0.9,0.9,0.6))
    
    data=data.frame(name,average,number)
    
    # Basic Barplot
    my_bar=barplot(data$average , border=F , names.arg=data$name , las=2 , col=colores1 , ylim=c(0,20) , main="popularidad por Rango Etario" )
    abline(v=c(4.9 , 9.7) , col="grey")
    
    # Add the text 
    text(my_bar, data$average+0.4 , paste("n = ",data$number,sep="") ,cex=1) 
    
    #Legende
    legend("topleft", legend = c(df_PopEdad[[2]][1],df_PopEdad[[2]][2],df_PopEdad[[2]][3],df_PopEdad[[2]][4] ), 
           col = colores1 , 
           bty = "n", pch=20 , pt.cex = 2, cex = 0.8, horiz = FALSE, inset = c(0.05, 0.05))
  })
  
  output$educacion <- renderPlot({
    df_educacion<-data.frame()
    df_educacion<-tablamaster()
    df_educacion<-df_rawdata1
    
    df_educacion <-aggregate(df_educacion$cantidad,by=list(df_educacion$desc_candidatos_presidenciales_mas_considerados,df_educacion$desc_nivel_de_estudios),FUN=sum)
    names(df_educacion)<-c("candidatosMenos","educacion","cantidad")
    df_educacionTipos <-aggregate(df_educacion$cantidad,by=list(df_educacion$educacion),FUN=sum)
    
    names(df_educacionTipos)<-c("Tipos","cantidad")
    
    name= df_educacion[[1]]
    average= df_educacion[[3]]
    number= df_educacion[[3]]
    
    data=data.frame(name,average,number)
    
    col1<-c(rgb(0.3,0.1,0.4,0.6) , rgb(0.3,0.2,0.4,0.6),rgb(0.3,0.3,0.4,0.6),rgb(0.3,0.4,0.4,0.6),rgb(0.2,0.5,0.4,0.6),rgb(0.3,0.6,0.4,0.6),rgb(0.3,0.7,0.4,0.6),rgb(0.3,0.8,0.4,0.6),rgb(0.3,0.9,0.4,0.6))
    
    # Basic Barplot
    my_bar=barplot(data$average , border=F , names.arg=data$name , las=2 , col=col1 , ylim=c(0,30) , main="popularidad por educacion" )
    abline(v=c(4.9 , 9.7) , col="grey")
    
    # Add the text 
    text(my_bar, data$average+0.4 , paste("n = ",data$number,sep="") ,cex=1) 
    
    #Legende
    legend("topleft", legend = c(df_educacionTipos$Tipos), 
           col = col1 , 
           bty = "n", pch=20 , pt.cex = 2, cex = 0.8, horiz = FALSE, inset = c(0.05, 0.05))
    
    
  })
  
  output$candidatosmasconsiderados <- renderPlot({
    df_tablamaster<-tablamaster()
    df_candidatosmasconsiderados<-data.frame()
    df_candidatosmasconsiderados<-aggregate(df_tablamaster$cantidad, list(df_tablamaster$desc_candidatos_presidenciales_mas_considerados),sum)
    names(df_candidatosmasconsiderados)<-c("candidatos", "cantidad")
    df_candidatosmasconsiderados$fraction = round(df_candidatosmasconsiderados[[2]] / sum(df_candidatosmasconsiderados[[2]] )*100, digits = 2)
    df_candidatosmasconsiderados$labels =paste( df_candidatosmasconsiderados$fraction,"%", sep=" ")
    df_candidatosmasconsiderados$labels <- paste(df_candidatosmasconsiderados$labels, "\n", df_candidatosmasconsiderados[[1]] , sep="")
    df_candidatosmasconsiderados = df_candidatosmasconsiderados[order(df_candidatosmasconsiderados$fraction), ]
    colores= brewer.pal(4, "BuPu") 
    require(plotrix)
    pie3D(df_candidatosmasconsiderados$fraction,labels = df_candidatosmasconsiderados$labels,explode = 0.1  ,  main = "% de popularidad", col= colores)

    
  })
  output$candidatosNeg <- renderPlot({
   # df_candidatoNeg<-dbGetQuery(con,"select desc_candidatos_presidenciales_menos_considerados candidatosMenos, count(1) cantidad from ft_encuesta e inner join lk_candidatos_presidenciales_menos_considerados g on (e.id_candidatos_presidenciales_menos_considerados=g.id_candidatos_presidenciales_menos_considerados) group by desc_candidatos_presidenciales_menos_considerados;")
    df_tablamaster<-tablamaster()
    df_candidatoNeg<-data.frame()
    df_candidatoNeg<-aggregate(df_tablamaster$cantidad, list(df_tablamaster$desc_candidatos_presidenciales_menos_considerados),sum)
    names(df_candidatoNeg)<-c("candidatosMenos", "cantidad")
    
    df_candidatoNeg$fraction = round(df_candidatoNeg[[2]] / sum(df_candidatoNeg[[2]] )*100, digits = 2)
    df_candidatoNeg$labels =paste( df_candidatoNeg$fraction,"%", sep=" ")
    df_candidatoNeg$labels <- paste(df_candidatoNeg$labels, "\n", df_candidatoNeg[[1]] , sep="")
    df_candidatoNeg = df_candidatoNeg[order(df_candidatoNeg$fraction), ]
    colores= brewer.pal(4, "BuPu") 
    require(plotrix)
    pie3D(df_candidatoNeg$fraction,labels = df_candidatoNeg$labels,explode = 0.1, main = "% de Candidatos Rechazados", col= colores)
    
    
  })  
  output$pop_genero <- renderPlot({
    df_PopGenero<-data.frame()
    df_PopGenero<-tablamaster()
    df_PopGenero <-aggregate(df_PopGenero$cantidad,by=c(df_PopGenero[21],df_PopGenero[14]),FUN=sum)
    names(df_PopGenero)<-c("candidatosMenos","desc_genero","cantidad")

    name= df_PopGenero[[1]]
    average= df_PopGenero[[3]]
    number= df_PopGenero[[3]]

    data=data.frame(name,average,number)
    
    # Basic Barplot
    my_bar=barplot(data$average , border=F , names.arg=data$name , las=2 , col=c(rgb(0.3,0.1,0.4,0.6) , rgb(0.3,0.5,0.4,0.6)) , ylim=c(0,30) , main="popularidad por sexo" )
    abline(v=c(4.9 , 9.7) , col="grey")
    
    # Add the text 
    text(my_bar, data$average+0.4 , paste("n = ",data$number,sep="") ,cex=1) 
    
    #Legende
    legend("topleft", legend = c(df_PopGenero[[2]][1],df_PopGenero[[2]][2] ), 
           col = c(rgb(0.3,0.1,0.4,0.6) , rgb(0.3,0.5,0.4,0.6) , rgb(0.3,0.9,0.4,0.6) ,  rgb(0.3,0.9,0.4,0.6)) , 
           bty = "n", pch=20 , pt.cex = 2, cex = 0.8, horiz = FALSE, inset = c(0.05, 0.05))
  }) 
#  output$RawData <- DT::renderDataTable(datos)
  
  # subset the records to the row that was clicked
  # drilldata <- reactive({
 #  shiny::validate(
  #    need(length(input$summary_rows_selected) > 0, "Select rows to drill down!")
  #  )    
 #    df_rawdata<-data.frame()
 #    df_rawdata<-tablamaster()
  #   FilaSelected<-datos[as.integer(input$summary_rows_selected), ]
  #   FilaSelected<-data.frame(datos)
  #   output$relatorio <- DT::renderDataTable(merge(df_rawdata,FilaSelected))
    
 # })
  
  # display the subsetted data
#  output$drilldown <- DT::renderDataTable(drilldata())
  

  output$table1 <- DT::renderDataTable({
    diamonds2<-data.frame()
    df_rawdata2<-data.frame()
    df_rawdata2<-tablamaster()
  #  c1<-input$show_vars
    c1<-input$check2
    c2<-paste(c1,collapse=",")
    diamonds2<-df_rawdata2
  #  R<-paste0(" diamonds2[, input$show_vars, drop = FALSE] %>% group_by(",c2,") %>% summarise(cantidad = sum(cantidad))")
    R<-paste0(" diamonds2 %>% group_by(",c2,") %>% summarise(cantidad = sum(cantidad))")
    
    D<-eval(parse(text=R))  
    
    output$downloadData <- downloadHandler('rawdata.csv',
      content = function(file) {
        write.csv(D, file)
      }
    )
    
    DT::datatable( D, filter = 'bottom', options = list(pageLength = 5, autoWidth = TRUE,scrollY = '300px', scrollX = TRUE) ) 
    
    
   })

  output$mytable3 <- renderHighchart({
    
  # backup<-df_rawdata
   # df_rawdata<-dbGetQuery(con,"select desc_candidatos_presidenciales_mas_considerados, g.desc_genero,RE.desc_rango_etario,H.desc_hijos, ES.desc_nivel_de_Estudios,FO.desc_nivel_economico_focos, IV.desc_tiene_intesion_De_votar, MC.desc_candidatos_presidenciales_menos_Considerados, cv.desc_posibilidad_De_cambio_de_voto, CQV.desc_concubinos_que_votan, pvc.desc_posibilidad_de_que_voten_al_mismo_candidato, count(1) cantidad from ft_encuesta e inner join lk_candidatos_presidenciales_mas_considerados c on (e.id_candidatos_presidenciales_mas_considerados=c.id_candidatos_presidenciales_mas_considerados)  inner join lk_genero g on (e.id_genero=g.id_genero)  inner join lk_rango_etario RE on (e.id_rango_etario=RE.id_rango_etario) inner join lk_hijos H on (e.id_hijos=H.id_hijos)  inner join lk_nivel_de_estudios ES on (e.id_nivel_de_estudios=ES.id_nivel_de_estudios) inner join lk_nivel_economico_focos FO on (e.id_nivel_economico_focos=FO.id_nivel_economico_focos) inner join lk_tiene_intesion_de_votar IV on (e.id_tiene_intesion_de_votar=IV.id_tiene_intesion_de_votar) inner join lk_posibilidad_de_cambio_de_voto CV  on (e.id_posibilidad_de_cambio_de_voto=CV.id_posibilidad_de_cambio_de_voto) inner join lk_candidatos_presidenciales_menos_considerados MC on (e.id_candidatos_presidenciales_menos_considerados= MC.id_candidatos_presidenciales_menos_considerados)  inner join lk_concubinos_que_votan CQV on (e.id_concubinos_que_votan=CQV.id_concubinos_que_votan)  inner join lk_posibilidad_de_que_voten_al_mismo_candidato PVC  on (e.id_posibilidad_de_que_voten_al_mismo_candidato=PVC.id_posibilidad_de_que_voten_al_mismo_candidato)  group by desc_candidatos_presidenciales_mas_considerados, g.desc_genero,RE.desc_rango_etario, H.desc_hijos, ES.desc_nivel_de_Estudios,FO.desc_nivel_economico_focos, IV.desc_tiene_intesion_De_votar, MC.desc_candidatos_presidenciales_menos_Considerados, cv.desc_posibilidad_De_cambio_de_voto, CQV.desc_concubinos_que_votan,pvc.desc_posibilidad_de_que_voten_al_mismo_candidato order by desc_candidatos_presidenciales_mas_considerados, g.desc_genero,RE.desc_rango_etario,H.desc_hijos, ES.desc_nivel_de_Estudios,FO.desc_nivel_economico_focos, IV.desc_tiene_intesion_De_votar, MC.desc_candidatos_presidenciales_menos_Considerados,cv.desc_posibilidad_De_cambio_de_voto, CQV.desc_concubinos_que_votan, pvc.desc_posibilidad_de_que_voten_al_mismo_candidato")
    df_rawdata<-data.frame()
    df_rawdata<-tablamaster()
    
    t<-length(df_rawdata)
    d0<-names(df_rawdata[,13:t])
    d1<-names(df_rawdata[,13:(t-1)])
    d2<-paste(d1,collapse=",")
    df_rawdata<-df_rawdata[,13:t]
    df_rawdata$cantidad<-as.numeric(df_rawdata$cantidad)
    R<-paste0(" df_rawdata[, d0, drop = FALSE] %>% group_by(",d2,") %>% summarize(cantidad = sum(cantidad))")
    df_rawdata<-eval(parse(text=R)) 
    df_rawdata<-t(na.omit(as.data.frame(t(df_rawdata))))
    df_rawdata<-data.frame(df_rawdata)
    d0<-names(df_rawdata)
    t<-length(df_rawdata)
    d1<-names(df_rawdata[,1:(t-1)])
    d2<-paste(d1,collapse=",")
    df_rawdata$cantidad<-as.numeric(df_rawdata$cantidad)
    R<-paste0(" df_rawdata %>% group_by(",d2,") %>% summarize(cantidad = sum(cantidad))")
    df_rawdata<-eval(parse(text=R)) 
    
    #####################pasar a character################
    columnas<-ncol(df_rawdata)
    x <- 1
    
    while(x < columnas ) {
      
      if(class(df_rawdata[[x]])=="factor") { 
        df_rawdata[[x]]<-as.character(df_rawdata[[x]])} 
      x <- x+1
    }
    
    
    #############################################33
    
    
    graficod<-aggregate(df_rawdata$cantidad, df_rawdata[1],sum)
    #graficod<-data.frame(graficod)
    names(graficod)<-c("x","y")
    df <- data_frame(
      name = c(graficod$x), #descripcion
      y = c(graficod$y),                       #id
      drilldown = name
    )
    
    hc <- highchart() %>%
      hc_chart(type = "column") %>%
      hc_title(text = "Drilldown de acuerdo al orden de la encuesta") %>%
      hc_xAxis(type = "category") %>%
      hc_legend(enabled = FALSE) %>%
      hc_plotOptions(
        series = list(
          boderWidth = 0,
          dataLabels = list(enabled = TRUE)
        )
      ) %>%
      hc_add_series(
        data = df,
        name = "Things",
        colorByPoint = TRUE
      )
    
    c1<-names(df_rawdata)
    padres<-""
    while(length(df_rawdata)>2){
      Totalcolumnas<-length(df_rawdata)
      filas<-1
      while(filas<= nrow(df_rawdata)) {
        padre<-df_rawdata[[Totalcolumnas-2]][filas]
        hijos<-""
        while(padre==df_rawdata[[Totalcolumnas-2]][filas] & filas<= nrow(df_rawdata) ){
          # calculo el drilldown
          i<-Totalcolumnas-1
          drill<-""
          while (i>=1){
            drill<-paste0(drill,df_rawdata[[i]][filas])
            i<-i-1
          }
          
          r<-names(df_rawdata[Totalcolumnas-1])
          r<-substr(r, 6, nchar(r))
          hijos<-paste0(hijos,"list(name ='",r,"-", df_rawdata[[Totalcolumnas-1]][filas],"', y = ",df_rawdata[[Totalcolumnas]][filas],",  drilldown = '",drill,"')")
          filas<-filas+1
          if (padre==df_rawdata[[Totalcolumnas-2]][filas] & filas<= nrow(df_rawdata) ) {hijos<-paste0(hijos,",")} 
        }
        #calculo id padre 
        i<-Totalcolumnas-2
        drill<-""
        while (i>=1){
          drill<-paste0(drill,df_rawdata[[i]][filas-1])
          i<-i-1
        }
        
        padres<-paste0(padres,"list( name = '",padre,"',id ='",drill,"', data = list( ",hijos,"))")
        if (filas<= nrow(df_rawdata)) {padres<-paste0(padres,",")} 
      }
      c2<-paste(c1[1:(Totalcolumnas-2)],collapse=",")
      R<-paste0("df_rawdata %>% group_by(",c2,") %>% summarize(cantidad=sum(cantidad))")
      df_rawdata<-eval(parse(text=R)) 
      if (length(df_rawdata)>2) {padres<-paste0(padres,",")} 
      
    }
    padres<-paste0("list(",padres,")")
    
    
    #fileConn<-file("output.txt")
    #writeLines(padres, fileConn)
    #close(fileConn)
    
    hc1 <- hc %>% hc_drilldown(
      series =eval(parse(text = padres)))
    
    #df_rawdata<-backup
    hc1
    

  })
  
  
  session$onSessionEnded(function(){
    dbDisconnect(con)
    dbUnloadDriver(pg)
  })
  
  
  #  datasetInput <- reactive({
   #   switch(input$dataset,
  #           "rock" = rock,
  #           "pressure" = pressure,
  #           "cars" = cars)
  #  })
    
  #  output$table <- renderTable({
  #    datasetInput()
  #  })
    
  #  output$downloadData <- downloadHandler(
  #    filename = function() { 
  #      paste(input$dataset, '.csv', sep='') 
   #   },
   #   content = function(file) {
   #     write.csv(datasetInput(), file)
   #   }
  #  )
  
  
  
  
})


