#
# This is the user-interface definition of a Shiny web application. You can
# run the application by clicking 'Run App' above.
#
# Find out more about building applications with Shiny here:
# 
#    http://shiny.rstudio.com/
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
library (shinyWidgets)

pg = dbDriver("PostgreSQL")
pw<-"postgres"
con = dbConnect(pg, user='postgres', password=pw, host="localhost", port= --, dbname="--")
rm(pw)
df_nombrescolumnas<-c("desc_formulario","id","fecha","lon" ,"lat" ,"desc_area_geo_basica", "desc_localidad", "desc_distrito_local",  "desc_distrito_federal", "desc_seccion","administrative_area_level_1","administrative_area_level_2", "desc_candidatos_presidenciales_mas_considerados", "desc_genero","desc_rango_etario","desc_hijos", "desc_nivel_de_estudios","desc_nivel_economico_focos", "desc_tiene_intesion_de_votar", "desc_posibilidad_de_cambio_de_voto","desc_candidatos_presidenciales_menos_considerados","desc_concubinos_que_votan", "desc_posibilidad_de_que_voten_al_mismo_candidato", "desc_nivel_economico_ingreso","desc_principal_problema_localidad","desc_segundo_principal_problema_localidad","desc_principal_problema_de_su_municipio" ,"desc_principal_problema_estado" ,"desc_principal_problema_pais")
df_formulario<-dbGetQuery(con,"select id_formulario, desc_formulario from lk_formulario")
df_mexico<-dbGetQuery(con,"select id_formulario, id, lon, lat,administrative_area_level_1,administrative_area_level_2 from aux_encuesta")

dbDisconnect(con)
dbUnloadDriver(pg)

######Elementos para seleccionar el padre del drill down
i<-1
c<-df_nombrescolumnas
padres<-"list("
while (i<length(df_nombrescolumnas) ){
  padres<-paste0(padres,c[i]," = ",toString(i),",");
  i<-i+1
}
padres<-paste0(padres,c[i]," = ",toString(i),")")
selected1<-eval(parse(text=padres)) 
######Elementos para formulario el padre del drill down
i<-1
r<-count(df_formulario)
r[[1]]
fomrularios<-"list("
while (i<r[[1]] ){
  fomrularios<-paste0(fomrularios, "'",df_formulario[[2]][i], "'"," = ",toString(df_formulario[[1]][i]),",");
  i<-i+1
}
fomrularios<-paste0(fomrularios, "'",df_formulario[[2]][i], "'"," = ",toString(df_formulario[[1]][i]),")")
selectedfomrularios<-eval(parse(text=fomrularios))



shinyUI(
  dashboardPage(
    dashboardHeader(title="Encuestas"),
    dashboardSidebar(
      #sliderInput("bins","Numero of breaks",1,100,50),
      sidebarMenu(
        menuItem("Dashboard",tabName = "dashboard"),
        menuSubItem("Distribucion de las encuestas",tabName = "distribucion"),
        menuSubItem("Analisis por Candidatos" ,tabName = "AnalisisporCandidatos"),
        menuSubItem("Resultados de las encuestas" ,tabName = "Problemas_sociales"),
        menuSubItem("Raw Data" ,tabName = "RawData")
        
        
      )),
    dashboardBody(
      tabItems(
        tabItem(tabName = "dashboard",width=12,height = 750,
                fluidRow( width=12,height = 750, 
                  div(width=12,height = 750,box(width=12,height = 750,tabsetPanel( id = 'dataset8',tabPanel('Seleccione los filtros del Dashboard'
                                                                                   ,tags$td(width=4,
                                                                                            box(  dateRangeInput("dates", label = "Date range"),
                                                                                                                                            checkboxGroupInput("Formulario", label = "Formulario", 
                                                                                                                                                               choices = selectedfomrularios),
                                                                                                                                            selectizeInput(
                                                                                                                                              'e1', 'Estado', choices = df_mexico$administrative_area_level_1, multiple = TRUE),
                                                                                                                                            selectizeInput(
                                                                                                                                              'e2', 'Municipio', choices = df_mexico$administrative_area_level_2, multiple = TRUE),
                                                                                                                                            actionButton(inputId = "action", label = "Action"),width=4)
                                                                                            )
                                                                                   , 
                                                                                                                                   
                                                                                 #  tags$td(#style="height:400px; width:100%; scrolling=yes",
                                                                                                                                   # width=8,height = 300,
                                                                                                                                    box(
                                                                                                                                      DT::dataTableOutput('resumen')# verbatimTextOutput('resumen')
                                                                                                                                        ,width=8,height = 300)
                                                                                                                                    #  )
                                                                                                                             
                  
                  )))
                ))

        ),
        tabItem(tabName = "distribucion",
                #h1("Distribucion de los encuestados"),
                fluidRow(  #tags$td(box(width=4,sidebarPanel(width=10, conditionalPanel('input.dataset === "Grafico1"')))),
                tags$td(box(width=12,height = 750,tabsetPanel( id = 'dataset1',tabPanel('Distribucion de los encuestados',leafletOutput("map",height = 600)))))
                )
        ),
        tabItem(tabName = "AnalisisporCandidatos",
                fluidRow(box(plotOutput("candidatosmasconsiderados"),width=4, height = 450),
                         box(plotOutput("candidatosNeg"),width=4, height = 450),
                         box(plotOutput("indecisos"),width=4, height = 450),
                         box(plotOutput("pop_genero"),width=4, height = 450),
                         box(plotOutput("pop_edad"),width=4, height = 450),
                         box(plotOutput("educacion"),width=4, height = 450)
                         
                )
        ),
        tabItem(tabName = "Problemas_sociales",
              #  h1("Necesidades Sociales"),
               # fluidRow(box(leafletOutput("Necesidad"),width=50, height = 500))
             # tabPanel('iris', highchartOutput('mytable3', height = "500px"))
             fluidRow(  
               tags$td(box(width=12,height = 750,tabsetPanel( id = 'dataset1',tabPanel('Resultados de las encuestas',highchartOutput('mytable3', height = "500px")))))
             )
        ),
        tabItem(tabName = "RawData",
                fluidRow(  
                  tags$td(box(width=12,height = 750,tabsetPanel( id = 'dataset2',tabPanel( 'Encuestas' ,
                                                                 tags$td(
                                                                   width = 20,
                                                                   dropdownButton(
                                                                     label = "Check some boxes", status = "default", width = 200,
                                                                     actionButton(inputId = "a2z", label = "Sort A to Z", icon = icon("sort-alpha-asc")),
                                                                     actionButton(inputId = "z2a", label = "Sort Z to A", icon = icon("sort-alpha-desc")),
                                                                     br(),
                                                                     actionButton(inputId = "all", label = "(Un)select all"),
                                                                     checkboxGroupInput(inputId = "check2", label = "Choose", choices = df_nombrescolumnas,selected = df_nombrescolumnas,width = 40)
                                                                   )
                                                                 ),#tags$td
                                                                 
                                                                 tags$td(box(width=12,height = 500,
                                                                             tabPanel('datos', DT::dataTableOutput('table1')))
                                                                 ),#tags$td
                                                                 downloadButton('downloadData', 'Download')
                  ))))
                              
                )#fluidrow

        )#tabitem
      )#cierra items
      
      
      
    )
  )
)
