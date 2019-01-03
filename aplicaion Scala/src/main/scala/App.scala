import ml.sparkling.graph.operators.OperatorsDSL._

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.util.IntParam
import java.util.Calendar
import java.time.Duration
import java.time.LocalDateTime


import org.apache.spark.graphx._
import org.apache.spark.graphx.util.GraphGenerators

import org.apache.spark.sql.SparkSession

import java.io.File

import  org.apache.hadoop.fs.{FileSystem,Path}

import org.apache.spark.graphx.GraphLoader
import com.centrality.kBC.KBetweenness



object App{


	case class Position(ID:Long, Daily_Sample_ID:Long, Taxi_ID:Long, Timestamp:String, Lon:Double, Lat:Double, V6:String, 
	  Speed:Double, Orientation:Double, Timestamp2:String, V10:String, Distrit:Int, V12:String, Vertex:Long, Trip_ID:String, Lagged_Vertex:Long)

	def parsePosition(str: String): Position = {
	  val line = str.split(",")
	  //        ID             Daily_Sample_ID  Taxi_ID       Timestamp     Lon           Lat               V6       Speed             Orientation      
	  Position(line(0).toLong, line(1).toLong, line(2).toLong, line(3), line(4).toDouble, line(5).toDouble, line(6), line(7).toDouble, line(8).toDouble, 
	    line(9), line(10), line(11).toInt, line(12), line(13).toLong, line(14), line(15).toLong)
	  //Timestamp2  V10     District        V12       Vertex           Trip_ID    Lagged_Vertex 
	}


	def main (args: Array[String]){

		val spark = SparkSession
	      .builder
	      .appName("Application")
	      .getOrCreate()
	    val sc = spark.sparkContext

	    if(args.length == 0 ) {
	    	println("COMMANDS HELP: " + 
		  		"\n\t To run Betweenness: -bt <k betweenness value> <files to process>" +
		  		"\n\t To run pageRank: -pr <convergence value> <files to process>" +
		  		"\n\t To run Eigen: -ei <files to process>" +
		  		"\n\t To run time test: -t")
	    	spark.stop()
	    	System.exit(1)
	    }
		
		for(i <- 0 to args.length-1) {
	    	args(i) match {
	    		case "-bt" =>
	    			runBetweenness(args(i+1), args(i+2))
	    		case "-ei" =>
	    			runEigen(args(i+1))
	    		case "-pr" =>
	    			runPageRank(args(i+1), args(i+2))
	    		case "-tc" =>
	    			//runTriangleCounting(args(i+1))
	    		case "-cc" =>
	    			//runConnectedComponents(args(i+1))
	    		case "-t" =>
	    			timeTest()
	    		case _ =>
	    		println("COMMANDS HELP: " + 
			  		"\n\t To run Betweenness: -bt <k betweenness value> <files to process>" +
			  		"\n\t To run pageRank: -pr <convergence value> <files to process>" +
			  		"\n\t To run Eigen: -ei <files to process>" +
			  		"\n\t To run time test: -t")
	    	}
	    }
	    println("CALCULATIONS ENDED")
		spark.stop()
	}


	def runEigen(l: String){

		var limit = 0

		try {
    		limit = l.toInt.asInstanceOf[Int]
    	} catch {
    		case e :Exception=> 
    			println(Calendar.getInstance().getTime().toString + ": SHOULD INTRODUCE: " + 
    				"\n \t - NUMBER OF FILES TO CALCULATE")
    			System.exit(1)
    	}

		println(Calendar.getInstance().getTime().toString + ": STARTING EIGEN: Files = " + limit)

		val spark = SparkSession
	      .builder
	      .appName("Application")
	      .getOrCreate()
	    val sc = spark.sparkContext

		println(Calendar.getInstance().getTime().toString + ": LOOKING FOR FILES")

		val fs=FileSystem.get(sc.hadoopConfiguration)
		@transient val files = fs.listStatus(new Path("/miguel/tesalonica"))


		for(i <- 0 to files.length-1 if i < limit) {

			var fileString = files(i).getPath.toString.split("/")(5)

			if(!fs.exists(new Path("/miguel/results/eigen/" + fileString + "_v")) && !fs.exists(new Path("/miguel/results/eigen/" + fileString + "_e"))){

				println(Calendar.getInstance().getTime().toString + ": FILE " + (i+1) + "/" + limit)
				println(Calendar.getInstance().getTime().toString + ": " + fileString)

				var routes: RDD[((Long, Long), Long)] = sc.emptyRDD

				val textRDD = sc.textFile(files(i).getPath.toString)
				val positionsRDD = textRDD.map(parsePosition)

				routes = positionsRDD.map(pos => ((pos.Vertex, pos.Lagged_Vertex), pos.ID))
			
				val edges = routes.map {
					case ((vertex, lagged_Vertex), id) =>Edge(vertex.toLong, lagged_Vertex.toLong, id.toLong)
				}
				val graph = Graph.fromEdges(edges, defaultValue = 0)
				
				var time1 = LocalDateTime.now

				val centralityGraph: Graph[Double, _] = graph.eigenvectorCentrality()			

				var duration = Duration.between(time1, LocalDateTime.now)
				println(Calendar.getInstance().getTime().toString + ":\t EIGEN ENDED IN " + 
						duration.toMinutes + "m (" + duration.getSeconds + "s)")

				println(Calendar.getInstance().getTime().toString + ": SAVING RESULTS! ") 
				centralityGraph.vertices.coalesce(1).saveAsTextFile("/miguel/results/eigen/" + fileString + "_v")
				centralityGraph.edges.coalesce(1).saveAsTextFile("/miguel/results/eigen/" + fileString + "_e")
			} else {
				println(Calendar.getInstance().getTime().toString + ": EIGEN CENTRALITY FROM "+ fileString + " ALREADY CALCULATED AND SAVED")
				println(Calendar.getInstance().getTime().toString + ": SKIPPING")
			}
		}

		println(Calendar.getInstance().getTime().toString + ": EIGEN ENDED")

	}


	def runBetweenness(k: String, l: String){

		var kValue = 0
		var limit = 0

		try {
    		kValue = k.toInt.asInstanceOf[Int]
    		limit = l.toInt.asInstanceOf[Int]
    	} catch {
    		case e :Exception=> 
    			println(Calendar.getInstance().getTime().toString + ": SHOULD INTRODUCE: " + 
					"\n \t - K VALUE TO CALCULATE BETWEENNESS"+
					"\n \t - NUMBER OF FILES TO CALCULATE")
    			System.exit(1)
	   	}


		println(Calendar.getInstance().getTime().toString + ": STARTING BETWEENNESS: Files = " + limit + " kValue = " + kValue)

		val spark = SparkSession
	      .builder
	      .appName("Application")
	      .getOrCreate()
	    val sc = spark.sparkContext
		
		println(Calendar.getInstance().getTime().toString + ": LOOKING FOR FILES")

		val fs=FileSystem.get(sc.hadoopConfiguration)
		@transient val files = fs.listStatus(new Path("/miguel/tesalonica"))


		for(i <- 0 to files.length-1 if i < limit) {

			var fileString = files(i).getPath.toString.split("/")(5)

			if(!fs.exists(new Path("/miguel/results/betweenness/" + fileString + "_v")) && !fs.exists(new Path("/miguel/results/betweenness/" + fileString + "_e"))){

				println(Calendar.getInstance().getTime().toString + ": FILE " + (i+1) + "/" + limit)
				println(Calendar.getInstance().getTime().toString + ": " + fileString)

				var routes: RDD[((Long, Long), Long)] = sc.emptyRDD

				val textRDD = sc.textFile(files(i).getPath.toString)
				val positionsRDD = textRDD.map(parsePosition)

				routes = positionsRDD.map(pos => ((pos.Vertex, pos.Lagged_Vertex), pos.ID))
			
				val edges = routes.map {
					case ((vertex, lagged_Vertex), id) =>Edge(vertex.toLong, lagged_Vertex.toLong, id.toLong)
				}

				println(Calendar.getInstance().getTime().toString + ": FILE N EDGES: " + edges.count)
				println(Calendar.getInstance().getTime().toString + ": FILE EDGES PARTITIONS: " + edges.partitions.size)


				println(Calendar.getInstance().getTime().toString + ": SPLITING EDGES FROM FILE IN 20")
				val splitEdges = edges.randomSplit(Array(0.05, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05,
													0.05, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05), seed = 12L)
				
				var time_all = LocalDateTime.now

				for(j <- 0 to 19){

					if(!fs.exists(new Path("/miguel/tmp/" + fileString + "_e_" + j)) && !fs.exists(new Path("/miguel/tmp/" + fileString + "_v_" + j))){

						println(Calendar.getInstance().getTime().toString + ": STARTING "+ (j+1) +"/20")
						println(Calendar.getInstance().getTime().toString + ":\t N EDGES: " + splitEdges(j).count)
						println(Calendar.getInstance().getTime().toString + ":\t CREATING GRAPH")
						val g = Graph.fromEdges(splitEdges(j), defaultValue = 0)

						println(Calendar.getInstance().getTime().toString + ":\t GRAPH N VERTICES: " + g.vertices.count)
						println(Calendar.getInstance().getTime().toString + ":\t GRAPH VERTICES PARTITIONS: " + g.vertices.partitions.size)
						println(Calendar.getInstance().getTime().toString + ":\t GRAPH N EDGES: " + g.edges.count)
						println(Calendar.getInstance().getTime().toString + ":\t GRAPH EDGES PARTITIONS: " + g.edges.partitions.size)

						var time1 = LocalDateTime.now
						
						println(Calendar.getInstance().getTime().toString + ":\t RUNNING BETWEENNESS CENTRALITY")
						val kBCGraph = KBetweenness.run(g, kValue)

						var duration = Duration.between(time1, LocalDateTime.now)
						println(Calendar.getInstance().getTime().toString + ":\t BETWEENNESS ENDED IN " + 
								duration.toMinutes + "m (" + duration.getSeconds + "s)")

						println(Calendar.getInstance().getTime().toString + ":\t SAVING " + (j+1) +"/20")
						kBCGraph.vertices.coalesce(1).saveAsTextFile("/miguel/tmp/" + fileString + "_v_" + j)
						kBCGraph.edges.coalesce(1).saveAsTextFile("/miguel/tmp/" + fileString + "_e_" + j)

						println(Calendar.getInstance().getTime().toString + ": "+ (j+1) +"/20 FINISHED")
					} else {
						println(Calendar.getInstance().getTime().toString + ": CENTRALITY "+ (j+1) +"/20 " + "ALREADY CALCULATED")
						println(Calendar.getInstance().getTime().toString + ": SKIPPING")
					}
				}

				var duratiom_all = Duration.between(time_all, LocalDateTime.now)
				println(Calendar.getInstance().getTime().toString + ":\t ALL 20 BETWEENNESS ENDED IN " + 
					duratiom_all.toMinutes + "m (" + duratiom_all.getSeconds + "s)")


				println(Calendar.getInstance().getTime().toString + ": JOINING")

				@transient val filesV = fs.listStatus(new Path("/miguel/tmp/" + fileString + "_v_" + 0))

				var vert = sc.textFile(filesV(1).getPath.toString).map{ line =>
				    val fields = line.substring(1, line.length-1).split(",")
				        ((fields(0).toLong, fields(1).toDouble))
				} 

				var vertCount = sc.textFile(filesV(1).getPath.toString).map{ line =>
				    val fields = line.substring(1, line.length-1).split(",")
				        ((fields(0).toLong, 1))
				} 

				for(j <- 1 to 19){

				    @transient val filesV = fs.listStatus(new Path("/miguel/tmp/" + fileString + "_v_" + j))

				    var auxVert = sc.textFile(filesV(1).getPath.toString).map{ line =>
				        val fields = line.substring(1, line.length-1).split(",")
				            ((fields(0).toLong, fields(1).toDouble))
				    } 

				    vert = vert.fullOuterJoin(auxVert).map {
				        case (id:Long, (Some(left), Some(right))) =>
				          (id, left + right)
				        case (id:Long, (None, Some(right)))=>
				          (id, right)
				        case (id:Long, (Some(left), None))=>
				          (id, left) 
				    }

				    var auxVertCount = sc.textFile(filesV(1).getPath.toString).map{ line =>
				        val fields = line.substring(1, line.length-1).split(",")
				            ((fields(0).toLong, 1))
				    }

				    vertCount = vertCount.fullOuterJoin(auxVertCount).map {
				        case (id:Long, (Some(left), Some(right))) =>
				          (id, left + 1 )
				        case (id:Long, (None, Some(right)))=>
				          (id, right)
				        case (id:Long, (Some(left), None))=>
				          (id, left) 
				    }
				}

				var meanVert = vert.fullOuterJoin(vertCount).map{
				    case(id, (Some(cent), Some(count))) =>
				        (id, cent/count)
				}

				var graph = Graph(meanVert, edges)
				println(Calendar.getInstance().getTime().toString + ":\tFINAL GRAPH N VERTICES: " + graph.vertices.count)
				println(Calendar.getInstance().getTime().toString + ":\tFINAL GRAPH VERTICES PARTITIONS: " + graph.vertices.partitions.size)
				println(Calendar.getInstance().getTime().toString + ":\tFINAL GRAPH N EDGES: " + graph.edges.count)
				println(Calendar.getInstance().getTime().toString + ":\tFINAL GRAPH EDGES PARTITIONS: " + graph.edges.partitions.size)


				println(Calendar.getInstance().getTime().toString + ": SAVING RESULTS! ") 
				graph.vertices.coalesce(1).saveAsTextFile("/miguel/results/betweenness/" + fileString + "_v")
				graph.edges.coalesce(1).saveAsTextFile("/miguel/results/betweenness/" + fileString + "_e")
			} else {
				println(Calendar.getInstance().getTime().toString + ": CENTRALITY FROM "+ fileString + " ALREADY CALCULATED AND SAVED")
				println(Calendar.getInstance().getTime().toString + ": SKIPPING")
			}
		}

		println(Calendar.getInstance().getTime().toString + ": BETWEENNESS ENDED")
	}


	def runPageRank(k: String, l :String){

		var pageValue = 0.0
		var limit = 0

		try {
    		pageValue = k.toDouble.asInstanceOf[Double]
    		limit = l.toInt.asInstanceOf[Int]
    	} catch {
    		case e :Exception=> 
    			println(Calendar.getInstance().getTime().toString + ": SHOULD INTRODUCE: " + 
					"\n \t - PAGE VALUE TO CALCULATE PAGE RANK"+
					"\n \t - NUMBER OF FILES TO CALCULATE")
    			System.exit(1)
	   	}

		println(Calendar.getInstance().getTime().toString + ": STARTING PAGE RANK: Files = " + limit + ", Value = " + pageValue)

		val spark = SparkSession
	      .builder
	      .appName("Application")
	      .getOrCreate()
	    val sc = spark.sparkContext

		println(Calendar.getInstance().getTime().toString + ": LOOKING FOR FILES")

		val fs=FileSystem.get(sc.hadoopConfiguration)
		@transient val files = fs.listStatus(new Path("/miguel/tesalonica"))

		for(i <- 0 to files.length-1 if i < limit) {

			var fileString = files(i).getPath.toString.split("/")(5)

			if(!fs.exists(new Path("/miguel/results/pageRank/" + fileString + "_v")) && !fs.exists(new Path("/miguel/results/pageRank/" + fileString + "_e"))){

				println(Calendar.getInstance().getTime().toString + ": FILE " + (i+1) + "/" + limit)
				println(Calendar.getInstance().getTime().toString + ": " + fileString)

				var routes: RDD[((Long, Long), Long)] = sc.emptyRDD

				val textRDD = sc.textFile(files(i).getPath.toString)
				val positionsRDD = textRDD.map(parsePosition)

				routes = positionsRDD.map(pos => ((pos.Vertex, pos.Lagged_Vertex), pos.ID))
			
				val edges = routes.map {
					case ((vertex, lagged_Vertex), id) =>Edge(vertex.toLong, lagged_Vertex.toLong, id.toLong)
				}

				val graph = Graph.fromEdges(edges, defaultValue = 0)

				var time1 = LocalDateTime.now

				val pageGraph = graph.pageRank(pageValue)			
				
				var duration = Duration.between(time1, LocalDateTime.now)
				println(Calendar.getInstance().getTime().toString + ":\t PAGERANK ENDED IN " + 
						duration.toMinutes + "m (" + duration.getSeconds + "s)")

				println(Calendar.getInstance().getTime().toString + ": SAVING RESULTS! ") 
				pageGraph.vertices.coalesce(1).saveAsTextFile("/miguel/results/pageRank/" + fileString + "_v")
				pageGraph.edges.coalesce(1).saveAsTextFile("/miguel/results/pageRank/" + fileString + "_e")
			} else {
				println(Calendar.getInstance().getTime().toString + ": PAGE RANK FROM "+ fileString + " ALREADY CALCULATED AND SAVED")
				println(Calendar.getInstance().getTime().toString + ": SKIPPING")
			}
		}

		println(Calendar.getInstance().getTime().toString + ": PAGE RANK ENDED")

	}


	def runTriangleCounting(l :String){

		var limit = 0

		try {
    		limit = l.toInt.asInstanceOf[Int]
    	} catch {
    		case e :Exception=> 
    			println(Calendar.getInstance().getTime().toString + ": SHOULD INTRODUCE: " + 
					"\n \t - NUMBER OF FILES TO CALCULATE")
    			System.exit(1)
	   	}


		println(Calendar.getInstance().getTime().toString + ": STARTING TRIANGLE COUNTING: Files = " + limit)

		val spark = SparkSession
	      .builder
	      .appName("Application")
	      .getOrCreate()
	    val sc = spark.sparkContext

		println(Calendar.getInstance().getTime().toString + ": LOOKING FOR FILES")

		val fs=FileSystem.get(sc.hadoopConfiguration)
		@transient val files = fs.listStatus(new Path("/miguel/tesalonica"))


		for(i <- 0 to files.length-1 if i < limit) {

			var fileString = files(i).getPath.toString.split("/")(5)

			if(!fs.exists(new Path("/miguel/results/triangleCounting/" + fileString + "_v")) && !fs.exists(new Path("/miguel/results/triangleCounting/" + fileString + "_e"))){

				println(Calendar.getInstance().getTime().toString + ": FILE " + (i+1) + "/" + limit)
				println(Calendar.getInstance().getTime().toString + ": " + fileString)

				//var positions: RDD[(Long, String)] = sc.emptyRDD
				var routes: RDD[((Long, Long), Long)] = sc.emptyRDD

				val textRDD = sc.textFile(files(i).getPath.toString)
				val positionsRDD = textRDD.map(parsePosition)

				//routes = routes.++(positionsRDD.map(pos => ((pos.Vertex, pos.Lagged_Vertex), pos.ID)))
				routes = positionsRDD.map(pos => ((pos.Vertex, pos.Lagged_Vertex), pos.ID))
			
				val edges = routes.map {
					case ((vertex, lagged_Vertex), id) =>Edge(vertex.toLong, lagged_Vertex.toLong, id.toLong)
				}

				val graph = Graph.fromEdges(edges, defaultValue = 0)

				val triangle = graph.triangleCount()			

				println(Calendar.getInstance().getTime().toString + ": SAVING RESULTS! ") 
				triangle.vertices.coalesce(1).saveAsTextFile("/miguel/results/triangleCounting/" + fileString + "_v")
				triangle.edges.coalesce(1).saveAsTextFile("/miguel/results/triangleCounting/" + fileString + "_e")
			} else {
				println(Calendar.getInstance().getTime().toString + ": TRIANGLE COUNTING FROM "+ fileString + " ALREADY CALCULATED AND SAVED")
				println(Calendar.getInstance().getTime().toString + ": SKIPPING")
			}
		}

		println(Calendar.getInstance().getTime().toString + ": TRIANGLE COUNTING ENDED")

	}



	def runConnectedComponents(l: String){

		var limit = 0

		try {
    		limit = l.toInt.asInstanceOf[Int]
    	} catch {
    		case e :Exception=> 
    			println(Calendar.getInstance().getTime().toString + ": SHOULD INTRODUCE: " + 
					"\n \t - NUMBER OF FILES TO CALCULATE")
    			System.exit(1)
	   	}


		println(Calendar.getInstance().getTime().toString + ": STARTING CONNECTED COMPONENTS: Files = " + limit)

		val spark = SparkSession
	      .builder
	      .appName("Application")
	      .getOrCreate()
	    val sc = spark.sparkContext

		println(Calendar.getInstance().getTime().toString + ": LOOKING FOR FILES")

		val fs=FileSystem.get(sc.hadoopConfiguration)
		@transient val files = fs.listStatus(new Path("/miguel/tesalonica"))


		for(i <- 0 to files.length-1 if i < limit) {

			var fileString = files(i).getPath.toString.split("/")(5)

			if(!fs.exists(new Path("/miguel/results/connectedComponents/" + fileString + "_v")) && !fs.exists(new Path("/miguel/results/connectedComponents/" + fileString + "_e"))){

				println(Calendar.getInstance().getTime().toString + ": FILE " + (i+1) + "/" + limit)
				println(Calendar.getInstance().getTime().toString + ": " + fileString)

				//var positions: RDD[(Long, String)] = sc.emptyRDD
				var routes: RDD[((Long, Long), Long)] = sc.emptyRDD

				val textRDD = sc.textFile(files(i).getPath.toString)
				val positionsRDD = textRDD.map(parsePosition)

				//routes = routes.++(positionsRDD.map(pos => ((pos.Vertex, pos.Lagged_Vertex), pos.ID)))
				routes = positionsRDD.map(pos => ((pos.Vertex, pos.Lagged_Vertex), pos.ID))
			
				val edges = routes.map {
					case ((vertex, lagged_Vertex), id) =>Edge(vertex.toLong, lagged_Vertex.toLong, id.toLong)
				}

				val graph = Graph.fromEdges(edges, defaultValue = 0)

				val connectedGraph = graph.connectedComponents()			

				println(Calendar.getInstance().getTime().toString + ": SAVING RESULTS! ") 
				connectedGraph.vertices.coalesce(1).saveAsTextFile("/miguel/results/connectedComponents/" + fileString + "_v")
				connectedGraph.edges.coalesce(1).saveAsTextFile("/miguel/results/connectedComponents/" + fileString + "_e")
			} else {
				println(Calendar.getInstance().getTime().toString + ": CONNECTED COMPONENTS FROM "+ fileString + " ALREADY CALCULATED AND SAVED")
				println(Calendar.getInstance().getTime().toString + ": SKIPPING")
			}
		}

		println(Calendar.getInstance().getTime().toString + ": CONNECTED COMPONENTS ENDED")

	}


	def timeTest(){

		println(Calendar.getInstance().getTime().toString + ": STARTING TEST")

		val spark = SparkSession
	      .builder
	      .appName("Application")
	      .getOrCreate()
	    val sc = spark.sparkContext
		
		println(Calendar.getInstance().getTime().toString + ": LOOKING FOR FILE")

		val fs=FileSystem.get(sc.hadoopConfiguration)
		@transient val files = fs.listStatus(new Path("/miguel/tesalonica"))

		var fileString = files(0).getPath.toString.split("/")(5)

		println(Calendar.getInstance().getTime().toString + ": " + fileString)

		var routes: RDD[((Long, Long), Long)] = sc.emptyRDD

		val textRDD = sc.textFile(files(0).getPath.toString)
		val positionsRDD = textRDD.map(parsePosition)

		routes = positionsRDD.map(pos => ((pos.Vertex, pos.Lagged_Vertex), pos.ID))
	
		val edges = routes.map {
			case ((vertex, lagged_Vertex), id) =>Edge(vertex.toLong, lagged_Vertex.toLong, id.toLong)
		}

		println(Calendar.getInstance().getTime().toString + ": STARTING BETWEENNESS TEST")
		println(Calendar.getInstance().getTime().toString + ": SPLITING EDGES FROM FILE IN 20")
		val splitEdges = edges.randomSplit(Array(0.05, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05,
													0.05, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05), seed = 12L)

		println(Calendar.getInstance().getTime().toString + ": CREATING GRAPH FROM FILES'S 1/10 PART")
		var g = Graph.fromEdges(splitEdges(0), defaultValue = 0)

		var time1 = LocalDateTime.now

		println(Calendar.getInstance().getTime().toString + ": RUNNING BETWEENNESS CENTRALITY")

		var betweennessGraph = KBetweenness.run(g, 3)

		var duration = Duration.between(time1, LocalDateTime.now)

		println(Calendar.getInstance().getTime().toString + ": BETWEENNESS ENDED IN " + 
			duration.toMinutes + "m (" + duration.getSeconds + "s)")

		// To make space in memory
		betweennessGraph = null


		// --------------------------------------------------------------------------------------
		println(Calendar.getInstance().getTime().toString + ": STARTING EIGEN TEST")
		println(Calendar.getInstance().getTime().toString + ": CREATING GRAPH FROM FILES")
		g = Graph.fromEdges(edges, defaultValue = 0)

		time1 = LocalDateTime.now

		println(Calendar.getInstance().getTime().toString + ": RUNNING EIGEN CENTRALITY")
		var eigenGraph: Graph[Double, _] = g.eigenvectorCentrality()		

		duration = Duration.between(time1, LocalDateTime.now)

		println(Calendar.getInstance().getTime().toString + ": EIGEN ENDED IN " + 
			duration.toMinutes + "m (" + duration.getSeconds + "s)")

		eigenGraph = null


		// --------------------------------------------------------------------------------------
		println(Calendar.getInstance().getTime().toString + ": STARTING PAGE RANK TEST")
		println(Calendar.getInstance().getTime().toString + ": CREATING GRAPH FROM FILES")
		g = Graph.fromEdges(edges, defaultValue = 0)

		time1 = LocalDateTime.now

		println(Calendar.getInstance().getTime().toString + ": RUNNING PAGE RANK")
		var ranks = g.pageRank(0.0001)

		duration = Duration.between(time1, LocalDateTime.now)

		println(Calendar.getInstance().getTime().toString + ": PAGE RANK ENDED IN " + 
			duration.toMinutes + "m (" + duration.getSeconds + "s)")
		ranks = null

	}

}




