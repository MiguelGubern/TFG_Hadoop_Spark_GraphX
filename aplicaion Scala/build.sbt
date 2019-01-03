name := "Project"

version := "1.0"

scalaVersion := "2.11.8"

resolvers ++= Seq(
	"Spark Packages Repo" at "http://dl.bintray.com/spark-packages/maven",
	"Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"
)

libraryDependencies ++= Seq(
	"org.apache.spark" %% "spark-sql" % "2.3.1",
	"org.apache.spark" %% "spark-graphx" % "2.3.1",
	"org.apache.spark" %% "spark-core" % "2.3.1",
	"dmarcous" % "spark-betweenness" % "1.0-s_2.10",
	"ml.sparkling" %% "sparkling-graph-api" % "0.0.7",
	"ml.sparkling" %% "sparkling-graph-operators" % "0.0.7"
)