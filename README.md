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
- all_times.txt -> fichero resultante de obtener los tiempos de ejecución desde los ficheros de logs de las ejecuciones
- speedup.R -> programa de generación de gráficos con los tiempos de ejecución.
- resultsJoin.R -> programa que recupera y une todos los resultados de las métricas en un solo fichero.
- map.r -> programa de generación de mapa geográfico con los resultados
- map.html -> mapa generado
