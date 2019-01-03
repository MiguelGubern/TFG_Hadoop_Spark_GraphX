library(gsubfn)
library(data.table)
library(ggplot2)

#fichero con todas los valores de tiempos
file <- "c:/Users/migue/Documents/results/speedup/all_times.txt"

# 5 TIPOS DE EJECUCIÃ“N:
#   - calculo 20 veces individiales de betweenness sobre 1/20 de fichero, eigen sobre 1 fichero y page rank sobre 1 fichero con minimo 2 ejecutores
#   - calculo 20 veces individiales de betweenness sobre 1/20 de fichero, eigen sobre 1 fichero y page rank sobre 1 fichero con maximo 2 ejecutores
#   - calculo de las 3 metricas para 15 ficheros seguidos en una misma app con Dynamic Allocation
#   - calculo de las 3 metricas para 15 ficheros seguidos en una misma app con Dynamic Allocation despues de reiniciar el cluster para limpiar todo tipo de logs, cachés...
#   - calculo de las 3 metricas para 15 ficheros seguidos en una misma app sin Dynamic Allocation,es decir, dejando que Yarn decida.

txt <- gsub("[()]", "", readLines(file))

segundos <-  strapplyc(readLines(file), ".+[(](.+)[s][)]", simplify = TRUE)


# bt_be = todos los tiempos de BETWEENNESS sobre 1/20 de fichero (cada fichero es un dia)
# bt_ber = el mismo calculo sobre los siguientes ficheros pero tras realizar un reinicio del cluster
# bt_bey = el mismo calculo sobre los siguientes ficheros pero sin utilizar Spark Dynamic Resource Allocation

# ei_ber = todos los tiempos de EIGEN sobre 1 fichero (cada fichero es un dia) tras reinicio del cluster
# ei_bey = el mismo calculo sobre los siguientes ficheros pero sin utilizar Spark Dynamic Resource Allocation

# ei_ber = todos los tiempos de PAGE RANK sobre 1 fichero (cada fichero es un dia) tras reinicio del cluster
# ei_bey = el mismo calculo sobre los siguientes ficheros pero sin utilizar Spark Dynamic Resource Allocation

# bt_d2 = calculo individual de 1/20 de fichero con BETWEENNESS y MAXIMO 2 nodos ejecutores activos
# bt_d7 = calculo individual de 1/20 de fichero con BETWEENNESS y MINIMO 2 nodos ejecutores activos

# ei_d2 = calculo individual de 1 fichero con EIGEN y MAXIMO 2 nodos ejecutores activos
# ei_d7 = calculo individual de 1 fichero con EIGEN y MINIMO 2 nodos ejecutores activos

# pr_d2 = calculo individual de 1 fichero con PAGE RANK y MAXIMO 2 nodos ejecutores activos
# pr_d7 = calculo individual de 1 fichero con PAGE RANK y MINIMO 2 nodos ejecutores activos

big_execution <- segundos[349:600]
bt_be <- big_execution[ c(rep(TRUE, 20), FALSE) ]

big_execution_restart <- segundos[1:315]
bt_ber <- big_execution_restart[ c(rep(TRUE, 20), FALSE) ]

big_execution_yarn <- segundos[601:915]
bt_bey <- big_execution_yarn[ c(rep(TRUE, 20), FALSE) ]


ei_ber <- segundos[317:331]
pr_ber <- segundos[333:347]

ei_bey <- segundos[917:931]
pr_bey <- segundos[933:947]


segs_d2 <- segundos[949:1003]
bt_d2 <- segs_d2[ c(TRUE, rep(FALSE, 2))]
ei_d2 <- segs_d2[ c(FALSE,TRUE, FALSE)]
pr_d2 <- segs_d2[ c(rep(FALSE, 2),TRUE)]

segs_d7 <- segundos[1004:1063]
bt_d7 <- segs_d7[ c(TRUE, rep(FALSE, 2))]
ei_d7 <- segs_d7[ c(FALSE,TRUE, FALSE)]
pr_d7 <- segs_d7[ c(rep(FALSE, 2),TRUE)]


# Data Tables para representar los datos en graficos

table_bt <- data.table(c("bt_d7", "bt_d2", "bt_be", "bt_ber", "bt_bey"), 
                       c(mean(as.numeric(bt_d7)), mean(as.numeric(bt_d2)),
                         mean(as.numeric(bt_be)), mean(as.numeric(bt_ber)), mean(as.numeric(bt_bey))))

table_ei <- data.table(c("ei_d7", "ei_d2","ei_ber", "ei_bey"), 
                       c(mean(as.numeric(ei_d7)), mean(as.numeric(ei_d2)),
                         mean(as.numeric(ei_ber)), mean(as.numeric(ei_bey))))

table_pr <- data.table(c("pr_d7", "pr_d2", "pr_ber", "pr_bey"), 
                       c(mean(as.numeric(pr_d7)), mean(as.numeric(pr_d2)),
                         mean(as.numeric(pr_ber)), mean(as.numeric(pr_bey))))

table_bt_be <- data.table(c(1:length(bt_be)), as.numeric(bt_be)) 
table_bt_ber <- data.table(c(1:length(bt_ber)), as.numeric(bt_ber)) 
table_bt_bey <- data.table(c(1:length(bt_bey)), as.numeric(bt_bey)) 

table_ei_ber <- data.table(c(1:length(ei_ber)), as.numeric(ei_ber)) 
table_ei_bey <- data.table(c(1:length(ei_bey)), as.numeric(ei_bey)) 

table_pr_ber <- data.table(c(1:length(pr_ber)), as.numeric(pr_ber))
table_pr_bey <- data.table(c(1:length(pr_bey)), as.numeric(pr_bey))


# linear_g_bt = grafico linear de los tiempos para cada 1/20 de fichero con BETWEENNESS
# linear_g_btr = grafico linear de los tiempos para cada 1/20 de fichero con BETWEENNESS tras reinicio del cluster
# linear_g_bty = grafico linear de los tiempos para cada 1/20 de fichero con BETWEENNESS sin Dynamic Allocation

# g_bt = grafico de barras con la media de los tiempos para cada tipo de ejecucion de BETWEENNESS
# g_ei = grafico de barras con la media de los tiempos para cada tipo de ejecucion de EIGEN
# g_pr = grafico de barras con la media de los tiempos para cada tipo de ejecucion de PAGE RANK

linear_g_bt <- ggplot(data=table_bt_be, aes(x=V1, y=V2)) +
                geom_line()+
                theme_minimal()
linear_g_bt

linear_g_btr <- ggplot(data=table_bt_ber, aes(x=V1, y=V2)) +
                  geom_line()+
                  theme_minimal()
linear_g_btr

linear_g_bty <- ggplot(data=table_bt_bey, aes(x=V1, y=V2)) +
                  geom_line()+
                  theme_minimal()
linear_g_bty


g_bt <- ggplot(data=table_bt, aes(x=V1, y=V2)) +
          geom_bar(stat="identity", fill="steelblue")+
          geom_text(aes(label=V2), vjust=-0.3, size=3)+
          theme_minimal()
g_bt

g_ei <- ggplot(data=table_ei, aes(x=V1, y=V2)) +
          geom_bar(stat="identity", fill="steelblue")+
          geom_text(aes(label=V2), vjust=-0.3, size=3)+
          theme_minimal()
g_ei

g_pr <- ggplot(data=table_pr, aes(x=V1, y=V2)) +
          geom_bar(stat="identity", fill="steelblue")+
          geom_text(aes(label=V2), vjust=-0.3, size=3)+
          theme_minimal()
g_pr


