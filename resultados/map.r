library(leaflet)
library(leaflet.extras)
library(plyr)
library(data.table)
library(RColorBrewer)

table <- fread(file = "C:/Users/migue/Documents/results/all_means_sds.csv", sep = ",", header = TRUE, showProgress = TRUE)

table_bt_mean = tail(subset(table, bt_mean>0)[order(subset(table, bt_mean>0)$bt_mean),], 10000)
#table_bt_sd = tail(subset(table, bt_sd>0)[order(subset(table, bt_sd>0)$bt_sd),], 10000)

table_ei_mean = tail(table[order(table$ei_mean),], 10000)
#table_ei_sd = tail(subset(table, ei_sd>0)[order(subset(table, ei_sd>0)$ei_sd),], 10000)

table_pr_mean = tail(table[order(table$pr_mean),], 10000)
#table_pr_sd = tail(subset(table, pr_sd>0)[order(subset(table, pr_sd>0)$pr_sd),], 10000)

bluecols <- brewer.pal(9, 'Blues')
newcol <- colorRampPalette(bluecols)

# Create a color palette with handmade bins.
bins_bt_mean = seq(min(table_bt_mean$bt_mean), max(table_bt_mean$bt_mean), length.out = 20)
#bins_bt_sd = seq(min(table_bt_sd$bt_sd), max(table_bt_sd$bt_sd), length.out = 20)

bins_ei_mean = seq(min(table_ei_mean$ei_mean), max(table_ei_mean$ei_mean), length.out = 20)
#bins_ei_sd = seq(min(table_ei_sd$ei_sd), max(table_ei_sd$ei_sd), length.out = 20)

bins_pr_mean = seq(min(table_pr_mean$pr_mean), max(table_pr_mean$pr_mean), length.out = 20)
#bins_pr_sd = seq(min(table_pr_sd$pr_sd), max(table_pr_sd$pr_sd), length.out = 20)


palette_bt_mean = colorBin(palette=newcol(20), domain=table_bt_mean, na.color="transparent", bins=bins_bt_mean)
#palette_bt_sd = colorBin(palette=newcol(20), domain=table_bt_sd, na.color="transparent", bins=bins_bt_sd)

palette_ei_mean = colorBin(palette=newcol(20), domain=table_ei_mean, na.color="transparent", bins=bins_ei_mean)
#palette_ei_sd = colorBin(palette=newcol(20), domain=table_ei_sd, na.color="transparent", bins=bins_ei_sd)

palette_pr_mean = colorBin(palette=newcol(20), domain=table_pr_mean, na.color="transparent", bins=bins_pr_mean)
#palette_pr_sd = colorBin(palette=newcol(20), domain=table_pr_sd, na.color="transparent", bins=bins_pr_sd)


# Prepar the text for the tooltip:
text_bt_mean=paste("ID del Vertice: ", table_bt_mean$V1, "<br/>", 
                   "Media: ", table_bt_mean$bt_mean, "<br/>", 
                   "Desviación Tipica: ", table_bt_mean$bt_sd, sep="") %>% lapply(htmltools::HTML)
#text_bt_sd =paste("ID del Vertice: ", table_bt_sd$V1, "<br/>", "Desviación típica centralidad betweenness: ", table_bt_sd$bt_sd, sep="") %>% lapply(htmltools::HTML)

text_ei_mean=paste("ID del Vertice: ", table_ei_mean$V1, "<br/>", 
                   "Media: ", table_ei_mean$ei_mean, "<br/>", 
                   "Desviación Tipica: ", table_ei_mean$ei_sd, sep="") %>% lapply(htmltools::HTML)
#text_ei_sd=paste("ID del Vertice: ", table_ei_sd$V1, "<br/>", "Desviación típica centralidad eigenvector: ", table_ei_sd$ei_sd, sep="") %>% lapply(htmltools::HTML)

text_pr_mean=paste("ID del Vertice: ", table_pr_mean$V1, "<br/>", 
                   "Media: ", table_pr_mean$pr_mean, "<br/>", 
                   "Desviación Tipica: ", table_pr_mean$pr_sd, sep="") %>% lapply(htmltools::HTML)
#text_pr_sd=paste("ID del Vertice: ", table_pr_sd$V1, "<br/>", "Desviación típica Page Rank: ", table_pr_sd$pr_sd, sep="") %>% lapply(htmltools::HTML)


# Final Map

map=leaflet(table) %>% 
      addProviderTiles("CartoDB.DarkMatter", "Stamen.TonerHybrid", group = "CartoDB") %>%
      addTiles(group = "OSM (default)") %>%
      addProviderTiles(providers$Stamen.Toner, group = "Toner") %>%
      ######## BT MEANS ########
      addCircleMarkers(table_bt_mean$Lon, table_bt_mean$Lat,
                     fillColor = ~palette_bt_mean(table_bt_mean$bt_mean), fillOpacity = 0.7, color="white", radius=6, stroke=FALSE,
                     label = text_bt_mean,
                     labelOptions = labelOptions( style = list("font-weight" = "normal", padding = "3px 8px"), textsize = "13px", direction = "auto"),
                     group = "Media Betweenness Centrality"
      ) %>%
      addLegend( pal=palette_bt_mean, values=table_bt_mean$bt_mean, opacity=0.9, title = "Betweenness Centrality", position = "bottomright", group = "Media Betweenness Centrality") %>%
      ######## EI MEANS ########
      addCircleMarkers(table_ei_mean$Lon, table_ei_mean$Lat,
                     fillColor = ~palette_ei_mean(table_ei_mean$ei_mean), fillOpacity = 0.7, color="white", radius=6, stroke=FALSE,
                     label = text_ei_mean,
                     labelOptions = labelOptions( style = list("font-weight" = "normal", padding = "3px 8px"), textsize = "13px", direction = "auto"),
                     group = "Media Eigenvector Centrality"
      ) %>%
      addLegend( pal=palette_ei_mean, values=table_ei_mean$ei_mean, opacity=0.9, title = "Eigenvector CEntrality", position = "bottomright", group = "Media Eigenvector Centrality") %>%
      ######## PR MEANS ########
      addCircleMarkers(table_pr_mean$Lon, table_pr_mean$Lat,
                     fillColor = ~palette_pr_mean(table_pr_mean$pr_mean), fillOpacity = 0.7, color="white", radius=6, stroke=FALSE,
                     label = text_pr_mean,
                     labelOptions = labelOptions( style = list("font-weight" = "normal", padding = "3px 8px"), textsize = "13px", direction = "auto"),
                     group = "Media Page Rank"
      ) %>%
      addLegend( pal=palette_pr_mean, values=table_pr_mean$pr_mean, opacity=0.9, title = "Page Rank", position = "bottomright", group = "Media Page Rank") %>%
      ######## LAYERS CONTROL #######
      addLayersControl(
        baseGroups = c("CartoDB","OSM (default)", "Toner"),
        overlayGroups = c("Media Betweenness Centrality", "Media Eigenvector Centrality", "Media Page Rank"),
        options = layersControlOptions(collapsed = TRUE, autoZIndex = TRUE),
        position = "bottomleft"
      ) %>%
      hideGroup(c("Media Eigenvector Centrality", "Media Page Rank"))
  
  