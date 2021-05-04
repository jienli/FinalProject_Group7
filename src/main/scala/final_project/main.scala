package final_project

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._
import org.apache.spark.storage.StorageLevel
import org.apache.log4j.{Level, Logger}


/*
              Testing verifyMIS
spark-submit --class "project_3.main" --master "local[*]" target/scala-2.12/project_3_2.12-1.0.jar verify ../project_3_data/small_edges.csv ../project_3_data/small_edges_MIS.csv
spark-submit --class "project_3.main" --master "local[*]" target/scala-2.12/project_3_2.12-1.0.jar verify ../project_3_data/small_edges.csv ../project_3_data/small_edges_non_MIS.csv

              Testing small_edges.csv
spark-submit --class "project_3.main" --master "local[*]" target/scala-2.12/project_3_2.12-1.0.jar compute ../project_3_data/small_edges.csv ourResults/our_small_edges_MIS
spark-submit --class "project_3.main" --master "local[*]" target/scala-2.12/project_3_2.12-1.0.jar verify ../project_3_data/small_edges.csv ourResults/our_small_edges_MIS/aaa.csv

              Testing line_100_edges.csv
spark-submit --class "project_3.main" --master "local[*]" target/scala-2.12/project_3_2.12-1.0.jar compute ../project_3_data/line_100_edges.csv ourResults/our_line_100_edges_MIS
spark-submit --class "project_3.main" --master "local[*]" target/scala-2.12/project_3_2.12-1.0.jar verify ../project_3_data/small_edges.csv ourResults/our_line_100_edges_MIS/aaa.csv

              Testing twitter_10000_edges.csv
spark-submit --class "project_3.main" --master "local[*]" target/scala-2.12/project_3_2.12-1.0.jar compute ../project_3_data/twitter_10000_edges.csv ourResults/our_twitter_10000_edges_MIS
spark-submit --class "project_3.main" --master "local[*]" target/scala-2.12/project_3_2.12-1.0.jar verify ../project_3_data/small_edges.csv ourResults/our_twitter_10000_edges_MIS/aaa.csv


Results are all good so far !!!
*/


object main{
  val rootLogger = Logger.getRootLogger()
  rootLogger.setLevel(Level.ERROR)

  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  Logger.getLogger("org.spark-project").setLevel(Level.WARN)





  def LubyMIS(g_in: Graph[Int, Int]): Graph[Int, Int] = {
    var g = g_in.mapVertices((id, vd) => (0.asInstanceOf[Int], 0.asInstanceOf[Float]))
    var remaining_vertices = 2.asInstanceOf[Long]
    val r = scala.util.Random
    var iteration = 0
    while (remaining_vertices >= 1) {
      // To Implement
      iteration += 1
      g = g.mapVertices((id, vd) => (vd._1, r.nextFloat()))
      val v = g.aggregateMessages[(Int, Float)](
        e => { 
          e.sendToDst(if ((e.srcAttr._2 + e.srcAttr._1) > (e.dstAttr._2 + e.dstAttr._1)) (0, 0) else (1, 0)); 
          e.sendToSrc(if ((e.srcAttr._2 + e.srcAttr._1) > (e.dstAttr._2 + e.dstAttr._1)) (1, 0) else (0, 0)) 
        }, 
        (msg1, msg2) => if (msg1._1 == 1 && msg2._1 == 1) (1, 0) else (0, 0)
      )

      val g2 = Graph(v, g.edges)
      
      val v2 = g2.aggregateMessages[(Int, Float)](
        e => {
          e.sendToDst(if (e.dstAttr._1 == 1) (1, 0) else (if (e.srcAttr._1 == 1) (-1, 0) else (0, 0))); 
          e.sendToSrc(if (e.srcAttr._1 == 1) (1, 0) else (if (e.dstAttr._1 == 1) (-1, 0) else (0, 0))) 
        }, 
        (msg1, msg2) => if (msg1._1 == 1 || msg2._1 == 1) (1, 0) else (if (msg1._1 == -1 || msg2._1 == -1) (-1, 0) else (0, 0))
      )

      g = Graph(v2, g.edges)
      g.cache()
      remaining_vertices = g.vertices.filter({case (id, x) => (x._1 == 0)} ).count()
    }
    println("************************************************************")
    println("Total number of Iteration = " + iteration)
    println("************************************************************")
    return g.mapVertices((id, vd) => (vd._1))
  }


  // version 1
  def IsraeliLtai(g_in: Graph[Int, Int]): Graph[Int, Int] = {
    var g = g_in.mapVertices((id, vd) => (1.asInstanceOf[Int]))
    var remaining_vertices = 2.asInstanceOf[Int]
    val r = scala.util.Random
    var iteration = 0
    while (remaining_vertices >= 1) {
      // To Implement
      iteration += 1

      // Original Version
      // g = g.mapVertices((id, vd) => (vd._1, r.nextFloat()))
      // //propose
      // val v = g.aggregateMessages[(Float, Int)](
      //   e => { 
      //     e.sendToDst(if (e.srcAttr._1 == 1) (e.srcAttr._2, e.srcId) else (0,0)); //(, ID!)
      //     e.sendToSrc(if (e.dstAttr._1 == 1) (e.dstAttr._2, e.dstId) else (0,0)) 
      //   }, 
      //   (msg1, msg2) => if (msg1._1 > msg2._1) (msg1._1, msg1._2) else (msg2._1, msg2._2)
      // ) //propose to v with max random value
      // val g2 = Graph(v, g.edges)
      // g2 = g2.mapVertices((id, vd) => (vd._2, if (r.nextInt()%2 == 0) 0 else 1 )) //(proposed vertex id, 0/1)


      // Uniform Random Propose Version
      g = g.mapVertices((id, vd) => (vd._1))
      //propose
      val v = g.aggregateMessages[((Int, Int), Int)](
        e => { 
          e.sendToDst(if (e.srcAttr._1 == 1) ((e.srcId, 1), e.dstAttr._1) else ((0,0), e.dstAttr._1)); //(, ID!)
          e.sendToSrc(if (e.dstAttr._1 == 1) ((e.dstId, 1), e.srcAttr._1) else ((0,0), e.srcAttr._1)) 
        }, 
        (msg1, msg2) => 
          if (r.nextFloat() < (msg1._1._2.asInstanceOf[Float] / (msg1._1._2 + msg2._1._2).asInstanceOf[Float])) 
            ((msg1._1._1, msg1._1._2 + msg2._1._2), msg1._2) 
          else 
            ((msg2._1._1, msg1._1._2 + msg2._1._2), msg2._2)
      ) //propose to v with max random value
      val g2 = Graph(v, g.edges)
      g2 = g2.mapVertices((id, vd) => (vd._1._1, if (r.nextInt()%2 == 0) 0 else 1 )) //(proposed vertex id, 0/1)


      val v2 = g2.aggregateMessages[Int](
        e => {
          e.sendToDst(if(e.dstId == e.srcAttr._1 && e.srcAttr._2 == 0 && e.dstAttr._2 == 1) e.srcId else 0);
          e.sendToSrc(if(e.srcId == e.dstAttr._1 && e.dstAttr._2 == 0 && e.srcAttr._2 == 1) e.dstId else 0)
        },
        (msg1, msg2) => if (msg1 > msg2) msg1 else msg2 // accept propsal from v with max id
      )
      
      val g3 = Graph(v2, g.edges)
      g3 = g3.mapTriplets(t => if(t.srcId == t.dstAttr || t.dstId == t.srcAttr) 1 else -1)

      g.cache()
      remaining_vertices = g.vertices.filter({case (id, x) => (x._1 == 0)}).count()
    }
    // return 
  }






  // VD att: Int. 1 = free/active, 0 = in M/inactive
  // ED att: Int. 1 = free/active, 0 = in M/inactive
  def colorCoding(g_in: Graph[Int, Int]): Graph[Int, Int] = {
    var g = g_in.mapVertices((id, vd) => (vd._1, 0.asInstanceOf[Int]))
    val r = scala.util.Random
    val k = 100
    val iteration = 2**(2*k+1)*(k+1)*log(10,k) 
    for (i <- 1 to k) {
      g = g.mapVertices((id, vd) => (vd._1, r.nextInt(2)))    // 1=blue  2=red
      
      val v = g.aggregateMessages[(Int, Int, Int)](
        e => { 
          e.sendToDst(if (e.srcAttr._1 == e.dstId && e.srcAttr._2 == e.dstAttr._2) (e.dstAttr._1, e.dstAttr._2, 0) else (e.dstAttr._1, e.dstAttr._2, 1));
          e.sendToSrc(if (e.dstAttr._1 == e.srcId && e.dstAttr._2 == e.srcAttr._2) (e.srcAttr._1, e.srcAttr._2, 0) else (e.srcAttr._1, e.srcAttr._2, 1)) 
        }, 
        (msg1, msg2) => (msg1._1, msg1._2, msg1._3 * msg2._3)
      ) // color coding. vertecies is out if 3rd attribute is 0
      
      val g2 = Graph(v, g.edges)

      val e = g2.mapTriplets[(Int, Int)] (
        t => {
          if (t.srcAttr._3 + t.dstAttr._3 == 2 && t.srcAttr._2 != t.dstAttr._2) (e.Attr, 1) else (e.Attr, 0)
        }
      )

      val remaining_v = v.filter({case (id, x) => (x._3 == 1)})
      val remaining_e = e.edges.filter({case (src, dst, x) => (x._2 == 1)})
      val g_bipartite = Graph(remaining_v, remaining_e)

      // TODO: color coding done, need to feed to the path augmentation
      val g_augmented = augmentation(g_bipartite)
      // TODO: merge with the original g
    }
  }



  // Graph[(pairID, color, 1),(???, 1)]
  def augmentation(g_in: Graph[(Int,Int,Int), (Int,Int)]): Graph[(Int,Int,Int), (Int,Int)] = {
    // TODO

  }














 

  def verifyMIS(g_in: Graph[Int, Int]): Boolean = {
    val my = g_in.aggregateMessages[Int](
      e => {
        e.sendToDst(if (e.dstAttr == 1 && e.srcAttr == 1) 2 else (if (e.dstAttr == -1 && e.srcAttr == -1) -1 else 0)); 
        e.sendToSrc(if (e.dstAttr == 1 && e.srcAttr == 1) 2 else (if (e.dstAttr == -1 && e.srcAttr == -1) -1 else 0))
      },
      (msg1, msg2) => if (msg1 == 2 || msg2 == 2) 2 else (if (msg1 == -1 && msg2 == -1) -1 else 0)
    )
    val count2 = my.filter(x => x._2 == 2).count()
    val count_1 = my.filter(x => x._2 == -1).count()

    return count2 == 0 && count_1 == 0
  }






  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("project_3")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder.config(conf).getOrCreate()
/* You can either use sc or spark */

    if(args.length == 0) {
      println("Usage: project_3 option = {compute, verify}")
      sys.exit(1)
    }
    if(args(0)=="compute") {
      if(args.length != 3) {
        println("Usage: project_3 compute graph_path output_path")
        sys.exit(1)
      }
      val startTimeMillis = System.currentTimeMillis()
      val edges = sc.textFile(args(1)).map(line => {val x = line.split(","); Edge(x(0).toLong, x(1).toLong , 1)} )
      val g = Graph.fromEdges[Int, Int](edges, 0, edgeStorageLevel = StorageLevel.MEMORY_AND_DISK, vertexStorageLevel = StorageLevel.MEMORY_AND_DISK)
      val g2 = LubyMIS(g)

      val endTimeMillis = System.currentTimeMillis()
      val durationSeconds = (endTimeMillis - startTimeMillis) / 1000
      println("==================================")
      println("Luby's algorithm completed in " + durationSeconds + "s.")
      println("==================================")

      val g2df = spark.createDataFrame(g2.vertices)
      g2df.coalesce(1).write.format("csv").mode("overwrite").save(args(2))
    }
    else if(args(0)=="verify") {
      if(args.length != 3) {
        println("Usage: project_3 verify graph_path MIS_path")
        sys.exit(1)
      }

      val edges = sc.textFile(args(1)).map(line => {val x = line.split(","); Edge(x(0).toLong, x(1).toLong , 1)} )
      val vertices = sc.textFile(args(2)).map(line => {val x = line.split(","); (x(0).toLong, x(1).toInt) })
      val g = Graph[Int, Int](vertices, edges, edgeStorageLevel = StorageLevel.MEMORY_AND_DISK, vertexStorageLevel = StorageLevel.MEMORY_AND_DISK)

      val ans = verifyMIS(g)
      if(ans)
        println("Yes")
      else
        println("No")
    }
    else
    {
        println("Usage: project_3 option = {compute, verify}")
        sys.exit(1)
    }
  }
}
