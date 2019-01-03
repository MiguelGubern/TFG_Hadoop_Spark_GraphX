# TFG_Hadoop_Spark_GraphX

Repositorio para códigos y documentos creados y usados en el Trabajo de Fin de Grado de Ingeniería Informática en la Universidad de Las Palmas de Gran Canaria

## Obtención de vértices y arcos
A partir de ficheros de dataset de posiciones GPS en una ciudad se obtienen vétices y arcos para la creación de grafos.
Ficheros:
- vertices_obtainer.R -> programa de obtención de vértices y arcos en R.
- taxi_20150101_235743.txt -> fichero de ejemplo del dataset.
- vertices_20150101_235743.csv -> fichero de ejemplo tras el proceso de obtención.

## Aplicación Scala
Directorio de programa Scala que calcula diferentes métricas de centralidad para grafos obtenidos del dataset.

## Resultados
Obtención de tiempos de ejecución para diferentes métricas y configuraciones del clúster Hadoop Spark para generación de gráficos estadadísticos.
Creación de un mapa con los resultados de los cálculos de las métricas de centralidad.
