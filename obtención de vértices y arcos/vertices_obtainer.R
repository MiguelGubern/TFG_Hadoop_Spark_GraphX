require(ggmap)
require(dplyr)
require(purrr)
require(tidyr)
require(lubridate)



# V1: sample id. es un valor único, para cada file
# V2: TAXI_ID. 795/919 levels diferentes. Juntos 959 levels
# V3: timestamp (milésimas)
# V4: LONGITUDE (degrees) (outliers)
# V5: LATITUDE (degrees) (outliers)
# V6: NI IDEA. es una variable que cambia suavemente a lo largo de un trayecto, y normalmente tiene valores bajos
# Min. 1st Qu. Median Mean 3rd Qu. Max.
# -3273 2 4 5 6 3274
# 
# V7: VELOCIDAD en Km/s
# 
# V7 casi seguro es la velocidad
# 
# V8: Orientation (0-359) (outliers)
# 
# V9: Timestamp (milésimas)
# 
# V10: siempre a 1. Debe de ser un flag de muestra válida.
# 
# V11: Distrito

get_vertex_ID <- function(Lon, Lat, zone, sampling_step){
  grid_ncols <- ceiling((zone[3] - zone[1])/sampling_step)
  col <- ceiling((Lon - zone[1])/sampling_step)
  row <- ceiling((Lat - zone[2])/sampling_step)
  #print(paste("col = ",col, " row= ",row))
  vertex_ID <- ((row-1)*grid_ncols + col)
  return (vertex_ID) #((row-1)*grid_ncols + col)
}

get_Lon_Lat <- function(vertex_ID, zone, sampling_step){
  #returns Lon and Lat for a vertex_ID (Grid element mid point)
  grid_ncols <- ceiling((zone[3] - zone[1])/sampling_step)
  
  row <- ceiling(vertex_ID/grid_ncols)
  col <- vertex_ID %% grid_ncols
  Lon <- zone[1] + (col-1)*sampling_step + sampling_step/2
  Lat <- zone[2] + (row-1)*sampling_step + sampling_step/2 #Middle point of each grid
  return (cbind(Lon, Lat))
  
}


calculate_edges <- function(source_file, destination_file){
  
  options(digits.secs=3)
  thessaloniki_0101 <- read.csv(file=source_file, header=F, sep="\t",stringsAsFactors = F)
  thessaloniki_0101$V3 <- as.POSIXct(strptime(thessaloniki_0101$V3, format = "%d/%m/%Y %H:%M:%OS"),tz="GMT")
  
  thessaloniki_0101$V2 <- as.factor(thessaloniki_0101$V2)
  thessaloniki_0101$V7 <- as.factor(thessaloniki_0101$V7)
  
  names(thessaloniki_0101) <- c("Daily_Sample_ID", "Taxi_ID", "Timestamp", "Lon", "Lat", "V6", "Speed", "Orientation", "Timestamp2",
                                "V10", "Distrit", "V12")
  
  zone <- c(22.82,40.49,23.00,40.71)
  
  thessaloniki_s <- subset(thessaloniki_0101, Lon>zone[1] & Lat>zone[2] & Lon<=zone[3] & Lat<=zone[4])
  
  
  sampling_step <- 1E-4 #submuestreo por celdas de 0.0001 grados (lat y lon)
  
  
  thessaloniki_s$Vertex <- get_vertex_ID(thessaloniki_s$Lon, thessaloniki_s$Lat, zone, sampling_step)
  #calculamos la "celda" en la que está cada observación, mirando las coordenadas gps
  thessaloniki_s<- mutate(thessaloniki_s, Trip_ID = paste(paste(day(thessaloniki_s$Timestamp),month(thessaloniki_s$Timestamp),sep="_"),Taxi_ID,sep="_"))
  #agrupamos las muestras por ruta diaria de cada taxi, en realidad, nos da igual que no sea diario, podr�???amos agrupar por taxi, simplemente
  thessaloniki_s2 <- thessaloniki_s %>% group_by(Trip_ID) %>% mutate(Lagged_Vertex = lag(Vertex))
  #Ponemos juntos en cada fila el nodo (vertex) actual y el anterior de cada ruta, para extraer arcos (edges)
  thessaloniki_s2 <- thessaloniki_s2[which(!is.na(thessaloniki_s2[,]$Lagged_Vertex)),]
  #limpieza de filas con huecos en los edges
  edges <- thessaloniki_s2[,c("Vertex","Lagged_Vertex")]
  #seleccionamos sólo las dos columnas que nos interesan
  write.table(thessaloniki_s2, file= destination_file, quote = FALSE, sep = ",", col.names = FALSE)
}

path <- "C:/Users/migue/Documents/.Tesalonica/"

allFiles <- list.files(path = path, pattern = ".*.txt")

allFilesSize = length(allFiles)
contador = 1

for (file in allFiles) {
  print(paste("Calculating edges:", file))
  print(paste(contador, "/", allFilesSize, sep = " "))
  contador = contador + 1
  
  calculate_edges(paste(path, file, sep = ""), paste(path, "tesalonica_v_e/vertices", substr(file, 5, 20), ".csv", sep = ""))
}