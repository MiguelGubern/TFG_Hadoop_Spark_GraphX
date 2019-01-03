library(dplyr)
library(purrr)
library(tidyr)
library(lubridate)
library(plyr)
library(bit64)
library(data.table)
library(matrixStats)


####################### JOINING ALL BETWEENNESS RESULTS ###########################

path <- "c:/Users/migue/Documents/results/betweenness/"
allFiles <- list.files(path = path, pattern = ".*_v")

file <- allFiles[1]
filePath <- paste(paste(path, file, sep=""), "/part-00000", sep="")

print(filePath)

txt <- gsub("[()]", "", readLines(filePath))
btTable = fread(text=txt, sep = ",")

for (i in 2:16){
  file <- allFiles[i]
  filePath <- paste(paste(path, file, sep=""), "/part-00000", sep="")
  
  print(filePath)
  
  txt <- gsub("[()]", "", readLines(filePath))
  auxTable <- fread(text=txt, sep = ",")
  
  btTable <- merge(btTable,auxTable,by="V1",all=TRUE)
  
  names(btTable)[ncol(btTable)] = paste("V2_", i, sep = "")
  
}

btTable$bt_mean = rowMeans(btTable[,2:7], na.rm = TRUE)

btTable$bt_sd = rowSds(data.matrix(btTable[,2:7]), na.rm = TRUE)


####################### JOINING ALL EIGEN RESULTS ###########################

path <- "c:/Users/migue/Documents/results/eigen/"
allFiles <- list.files(path = path, pattern = ".*_v")

file <- allFiles[1]
filePath <- paste(paste(path, file, sep=""), "/part-00000", sep="")

print(filePath)

txt <- gsub("[()]", "", readLines(filePath))
eiTable = fread(text=txt, sep = ",")

for (i in 2:16){
  file <- allFiles[i]
  filePath <- paste(paste(path, file, sep=""), "/part-00000", sep="")
  
  print(filePath)
  
  txt <- gsub("[()]", "", readLines(filePath))
  auxTable <- fread(text=txt, sep = ",")
  
  eiTable <- merge(eiTable,auxTable,by="V1",all=TRUE)
  
  names(eiTable)[ncol(eiTable)] = paste("V2_", i, sep = "")
  
}

eiTable$ei_mean = rowMeans(eiTable[,2:17], na.rm = TRUE)

eiTable$ei_sd = rowSds(data.matrix(eiTable[,2:17]), na.rm = TRUE)



####################### JOINING ALL PAGERANK RESULTS ###########################

path <- "c:/Users/migue/Documents/results/pageRank/"
allFiles <- list.files(path = path, pattern = ".*_v")

file <- allFiles[1]
filePath <- paste(paste(path, file, sep=""), "/part-00000", sep="")

print(filePath)

txt <- gsub("[()]", "", readLines(filePath))
prTable = fread(text=txt, sep = ",")

for (i in 2:16){
  file <- allFiles[i]
  filePath <- paste(paste(path, file, sep=""), "/part-00000", sep="")
  
  print(filePath)
  
  txt <- gsub("[()]", "", readLines(filePath))
  auxTable <- fread(text=txt, sep = ",")
  
  prTable <- merge(prTable,auxTable,by="V1",all=TRUE)
  
  names(prTable)[ncol(prTable)] = paste("V2_", i, sep = "")
  
}

prTable$pr_mean = rowMeans(prTable[,2:17], na.rm = TRUE)

prTable$pr_sd = rowSds(data.matrix(prTable[,2:17]), na.rm = TRUE)


####################### TABLE MERGING AND SAVING ###########################

table <- merge(eiTable[,c(1,18,19)], prTable[, c(1,18,19)], by="V1", all=TRUE)
table <- merge(table, btTable[, c(1,18,19)], by="V1", all=TRUE)

get_Lon_Lat <- function(vertex_ID, zone, sampling_step){
  #returns Lon and Lat for a vertex_ID (Grid element mid point)
  grid_ncols <- ceiling((zone[3] - zone[1])/sampling_step)
  
  row <- ceiling(vertex_ID/grid_ncols)
  col <- vertex_ID %% grid_ncols
  Lon <- zone[1] + (col-1)*sampling_step + sampling_step/2
  Lat <- zone[2] + (row-1)*sampling_step + sampling_step/2 #Middle point of each grid
  return (cbind(Lon, Lat))
  
}

table$Lon <- get_Lon_Lat(as.double(table$V1), c(22.82,40.49,23.00,40.71), 1E-6)[,1]
table$Lat <- get_Lon_Lat(as.double(table$V1), c(22.82,40.49,23.00,40.71), 1E-6)[,2]

write.table(table, file = "C:/Users/migue/Documents/results/all_means_sds.csv", sep = ",",
            na = "", col.names = TRUE, row.names = FALSE)
