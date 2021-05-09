package final_project

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._
import org.apache.spark.storage.StorageLevel
import org.apache.log4j.{Level, Logger}

object main{
  val rootLogger = Logger.getRootLogger()
  rootLogger.setLevel(Level.ERROR)

  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  Logger.getLogger("org.spark-project").setLevel(Level.WARN)


  def Is(g_in: Graph[Int, Int]): RDD[Edge[Int]] = {
    var g:Graph[(Long,Float),Int] = g_in.mapVertices((id, vd) => (1.asInstanceOf[Long], 0.asInstanceOf[Float]))
    var remaining_vertices = 0.asInstanceOf[Int]
    var new_remaining = g.vertices.filter({case (id, x) => (x._1 == 1)}).count().asInstanceOf[Int]
    val r = scala.util.Random
    var iteration = 0
    // var result = g.edges.filter({case x => (x.attr == 1)})
    while (remaining_vertices != new_remaining) {
      // To Implement
      iteration += 1
      // g = g.mapVertices((id, vd) => (vd._1, r.nextFloat()))
      //propose
      var v = g.aggregateMessages[(Long, Float)](
        e => { 
          e.sendToDst(if (e.srcAttr._1 == 1 && e.dstAttr._1 == 1) (e.srcId, r.nextFloat()) else (-1,0)); //(ID,random)
          e.sendToSrc(if (e.dstAttr._1 == 1 && e.srcAttr._1 == 1) (e.dstId, r.nextFloat()) else (-1,0)) 
        }, 
        (msg1, msg2) => if (msg1._2 > msg2._2) msg1 else msg2
      ) //propose to v with max random value

      g = Graph(v, g.edges)
      g = g.mapVertices((id, vd) => (vd._1, if (r.nextInt()%2 == 0) 0 else 1 )) //(proposed vertex id, 0/1)
      
      v = g.aggregateMessages[(Long, Float)](
        e => {
          e.sendToDst(if(e.dstId == e.srcAttr._1 && e.srcAttr._2 == 0 && e.dstAttr._2 == 1) (e.srcId,0) else (-1,0));
          e.sendToSrc(if(e.srcId == e.dstAttr._1 && e.dstAttr._2 == 0 && e.srcAttr._2 == 1) (e.dstId,0) else (-1,0))
        },
        (msg1, msg2) => if (msg1._1 > msg2._1) msg1 else msg2 // accept propsal from v with max id
      )
      
      g = Graph(v, g.edges)
      g = g.mapTriplets(t => if( t.attr == 1 || t.srcId == t.dstAttr._1 || t.dstId == t.srcAttr._1) 1 else 0)

      g = Graph(g.vertices, g.edges)
      v = g.aggregateMessages[(Long, Float)](
        e => {
          e.sendToDst(if(e.attr == 1) (0,0) else (e.dstAttr._1, 0) );
          e.sendToSrc(if(e.attr == 1) (0,0) else (e.srcAttr._1, 0) )
        },
        (msg1, msg2) => if (msg1._1 == 0 || msg2._1 == 0) (0,0) else (1,0)
      ) //0=deactivate

      g = Graph(v, g.edges) 
      g.cache()
      remaining_vertices = new_remaining
      var test = g.edges.filter({case x => (x.attr == 1)}).count()
      new_remaining = g.vertices.filter({case (id, x) => (x._1 == 1)}).count().asInstanceOf[Int] //1
      println("************************************************************")
      println("Current Iteration = " + iteration + ". Remaining vertices = " + remaining_vertices + ". test: " + test)
      println("************************************************************")
    }


    return augmentation(g.mapVertices((id, vd) => vd._1.asInstanceOf[Int]))
  }








  // ED.attr (1=M, 0 = not M)
  // VD.attr (not in use)
  def augmentation(g_in: Graph[Int, Int]): RDD[Edge[Int]] = {
    println("\n\n================================================================")
    println("Starting Length 3 Augmentation ")
    println("================================================================")
    val r = scala.util.Random
    // var g:Graph[Int,Int] = g_in.mapVertices((id, vd) => (r.nextInt(4) + 1))          // don't know how or why to use this 1234 mapping
    var v = g_in.aggregateMessages[(Int, Long, Float)](
        e => {
          e.sendToDst((e.attr, 0, 0));
          e.sendToSrc((e.attr, 0, 0))
        },
        (msg1, msg2) => (msg1._1 | msg2._1, 0, 0)
      ) //Map ED attr to: 1 = M, 0 = not in M
    var g = Graph(v, g_in.edges) 

    var iteration = 0

    var matchedCount = 0.asInstanceOf[Long]
    var new_matchedCount = g.edges.filter({case x => (x.attr == 1)}).count().asInstanceOf[Long]
    var notProgressingStreak = 0
    while(notProgressingStreak < 2){

      // (In M?, self ID, random number)
      v = g.aggregateMessages[(Int, Long, Float)](
        e => {
          e.sendToDst(if(e.srcAttr._1 == 1 && e.dstAttr._1 == 0) (e.dstAttr._1, e.srcId, r.nextFloat()) else (e.dstAttr._1, e.srcId, -1) );
          e.sendToSrc(if(e.dstAttr._1 == 1 && e.srcAttr._1 == 0) (e.srcAttr._1, e.dstId, r.nextFloat()) else (e.srcAttr._1, e.dstId, -1) )
        },
        (msg1, msg2) => if (msg1._3 > msg2._3) msg1 else msg2
      ) //from 2 3 to 1 4, fro them to select one to propose; 1,4 select one 
      g = Graph(v, g.edges) 

      v = g.aggregateMessages[(Int, Long, Float)](
        e => {
          e.sendToDst(if(e.srcAttr._3 > 0 && e.srcAttr._2 == e.dstId) (e.dstAttr._1, e.srcId, r.nextFloat()) else e.dstAttr );
          e.sendToSrc(if(e.dstAttr._3 > 0 && e.dstAttr._2 == e.srcId) (e.srcAttr._1, e.dstId, r.nextFloat()) else e.srcAttr )
        },
        (msg1, msg2) => if (msg1._3 > msg2._3) msg1 else msg2
      ) //from 1 4 to 2 3, propose; 2,3 select one to accept
      g = Graph(v, g.edges) 

      v = g.aggregateMessages[(Int, Long, Float)](
        e => {
          // e.sendToDst(if(e.attr == 1 && e.srcAttr._3 > 0 && e.dstAttr._3 > 0) e.dstAttr else (e.dstAttr._1, e.dstAttr._2, -1) );
          // e.sendToSrc(if(e.attr == 1 && e.srcAttr._3 > 0 && e.dstAttr._3 > 0) e.srcAttr else (e.srcAttr._1, e.srcAttr._2, -1) )
          e.sendToDst(if(e.attr == 1 && (e.srcAttr._3 < 0 || e.dstAttr._3 < 0)) (e.dstAttr._1, e.dstAttr._2, -1) else e.dstAttr );
          e.sendToSrc(if(e.attr == 1 && (e.srcAttr._3 < 0 || e.dstAttr._3 < 0)) (e.srcAttr._1, e.srcAttr._2, -1) else e.srcAttr )
        },
        (msg1, msg2) => if (msg1._3 < msg2._3) msg1 else msg2
      ) //from 2 to 3 and 3 to 2, exchange info see if both are proposed to; 
      g = Graph(v, g.edges) 

      g = g.mapTriplets(t => 
          if (t.srcAttr._3 > 0 && t.dstAttr._3 > 0 && t.srcAttr._2 == t.dstId && t.dstAttr._2 == t.srcId) 
            1
          // if (t.srcAttr._3 > 0 && t.dstAttr._3 > 0 && t.srcAttr._2 == t.dstId && t.dstAttr._2 == t.srcId) 
          //   2
          else if (t.srcAttr._3 > 0 && t.dstAttr._3 > 0 && t.attr == 1)
            0
          else 
            t.attr
        )
      // Edges are now all up to date. need to uodate vertices attr based on edge attr

      v = g.aggregateMessages[(Int, Long, Float)](
        e => {
          e.sendToDst((e.attr, 0, 0));
          e.sendToSrc((e.attr, 0, 0))
        },
        (msg1, msg2) => (msg1._1 | msg2._1, 0, 0)
      ) //Map ED attr to: 1 = M, 0 = not in M
      g = Graph(v, g.edges) 
      g.cache()

      iteration += 1
      matchedCount = new_matchedCount
      new_matchedCount = g.edges.filter({case x => (x.attr == 1)}).count()
      println("************************************************************")
      println("Current Iteration = " + iteration + ". # matches: " + new_matchedCount)
      println("************************************************************")

      if ((new_matchedCount - matchedCount).asInstanceOf[Float] / new_matchedCount.asInstanceOf[Float] < 0.02) {
        notProgressingStreak += 1
      } else {
        notProgressingStreak = 0
      }
    }

    return g.edges.filter({case x => (x.attr == 1)})
  }
  

      // // Uniform Random Propose Version
      // g = g.mapVertices((id, vd) => (vd._1))
      // //propose
      // val v = g.aggregateMessages[((Int, Int), Int)](
      //   e => { 
      //     e.sendToDst(if (e.srcAttr._1 == 1) ((e.srcId, 1), e.dstAttr._1) else ((0,0), e.dstAttr._1)); //(, ID!)
      //     e.sendToSrc(if (e.dstAttr._1 == 1) ((e.dstId, 1), e.srcAttr._1) else ((0,0), e.srcAttr._1)) 
      //   }, 
      //   (msg1, msg2) => 
      //     if (r.nextFloat() < (msg1._1._2.asInstanceOf[Float] / (msg1._1._2 + msg2._1._2).asInstanceOf[Float])) 
      //       ((msg1._1._1, msg1._1._2 + msg2._1._2), msg1._2) 
      //     else 
      //       ((msg2._1._1, msg1._1._2 + msg2._1._2), msg2._2)
      // ) //propose to v with max random value
      // val g2 = Graph(v, g.edges)
      // g2 = g2.mapVertices((id, vd) => (vd._1._1, if (r.nextInt()%2 == 0) 0 else 1 )) //(proposed vertex id, 0/1)
      
  // VD att: Int. 1 = free/active, 0 = in M/inactive
  // ED att: Int. 1 = free/active, 0 = in M/inactive
  // def colorCoding(g_in: Graph[Int, Int]): Graph[Int, Int] = {
  //   var g = g_in.mapVertices((id, vd) => (vd._1, 0.asInstanceOf[Int]))
  //   val r = scala.util.Random
  //   val k = 100
  //   val iteration = 2**(2*k+1)*(k+1)*log(10,k) 
  //   for (i <- 1 to k) {
  //     g = g.mapVertices((id, vd) => (vd._1, r.nextInt(2)))    // 1=blue  2=red
      
  //     val v = g.aggregateMessages[(Int, Int, Int)](
  //       e => { 
  //         e.sendToDst(if (e.srcAttr._1 == e.dstId && e.srcAttr._2 == e.dstAttr._2) (e.dstAttr._1, e.dstAttr._2, 0) else (e.dstAttr._1, e.dstAttr._2, 1));
  //         e.sendToSrc(if (e.dstAttr._1 == e.srcId && e.dstAttr._2 == e.srcAttr._2) (e.srcAttr._1, e.srcAttr._2, 0) else (e.srcAttr._1, e.srcAttr._2, 1)) 
  //       }, 
  //       (msg1, msg2) => (msg1._1, msg1._2, msg1._3 * msg2._3)
  //     ) // color coding. vertecies is out if 3rd attribute is 0
      
  //     val g2 = Graph(v, g.edges)

  //     val e = g2.mapTriplets[(Int, Int)] (
  //       t => {
  //         if (t.srcAttr._3 + t.dstAttr._3 == 2 && t.srcAttr._2 != t.dstAttr._2) (e.Attr, 1) else (e.Attr, 0)
  //       }
  //     )

  //     val remaining_v = v.filter({case (id, x) => (x._3 == 1)})
  //     val remaining_e = e.edges.filter({case (src, dst, x) => (x._2 == 1)})
  //     val g_bipartite = Graph(remaining_v, remaining_e)

  //     // TODO: color coding done, need to feed to the path augmentation
  //     val g_augmented = augmentation(g_bipartite)
  //     // TODO: merge with the original g
  //   }
  // }

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("project_3")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder.config(conf).getOrCreate()
/* You can either use sc or spark */

    if(args.length == 0) {
      println("Usage: final_proj option = {compute, verify}")
      sys.exit(1)
    }
    if(args(0)=="compute") {
      if(args.length != 3) {
        println("Usage: final_proj compute graph_path output_path")
        sys.exit(1)
      }
      val startTimeMillis = System.currentTimeMillis()
      val edges = sc.textFile(args(1)).map(line => {val x = line.split(","); Edge(x(0).toLong, x(1).toLong , 0)} )
      val g = Graph.fromEdges[Int, Int](edges, 0, edgeStorageLevel = StorageLevel.MEMORY_AND_DISK, vertexStorageLevel = StorageLevel.MEMORY_AND_DISK)
      val g2 = Is(g)

      val endTimeMillis = System.currentTimeMillis()
      val durationSeconds = (endTimeMillis - startTimeMillis) / 1000
      println("==================================")
      println("Israeli-Itai's algorithm (& 3-Augmentation) completed in " + durationSeconds + "s.")
      println("==================================")

      val g2df = spark.createDataFrame(g2) //g2.vertices
      g2df.coalesce(1).write.format("csv").mode("overwrite").save(args(2))
    }
    // else if(args(0)=="verify") {
    //   if(args.length != 3) {
    //     println("Usage: project_3 verify graph_path MIS_path")
    //     sys.exit(1)
    //   }

    //   val edges = sc.textFile(args(1)).map(line => {val x = line.split(","); Edge(x(0).toLong, x(1).toLong , 1)} )
    //   val vertices = sc.textFile(args(2)).map(line => {val x = line.split(","); (x(0).toLong, x(1).toInt) })
    //   val g = Graph[Int, Int](vertices, edges, edgeStorageLevel = StorageLevel.MEMORY_AND_DISK, vertexStorageLevel = StorageLevel.MEMORY_AND_DISK)

    //   val ans = verifyMIS(g)
    //   if(ans)
    //     println("Yes")
    //   else
    //     println("No")
    // }
    else
    {
        println("Usage: final_proj option = {compute, verify}")
        sys.exit(1)
    }
  }
}


/*
spark-submit --class "final_project.main" --master "local[*]" target/scala-2.12/project_3_2.12-1.0.jar compute  data/log_normal_100.csv data/log_normal_100_matching.csv

spark-submit --master "local[*]" --class "final_project.verifier" target/scala-2.12/project_3_2.12-1.0.jar data/log_normal_100.csv data/log_normal_100_matching.csv
48



spark-submit --class "final_project.main" --master "local[*]" target/scala-2.12/project_3_2.12-1.0.jar compute  data/musae_ENGB_edges.csv data/musae_ENGB_edges_matching.csv

spark-submit --master "local[*]" --class "final_project.verifier" target/scala-2.12/project_3_2.12-1.0.jar data/musae_ENGB_edges.csv data/musae_ENGB_edges_matching.csv
The matched edges form a matching of size: 2350



spark-submit --class "final_project.main" --driver-memory 12g --master "local[*]" target/scala-2.12/project_3_2.12-1.0.jar compute  data/soc-pokec-relationships.csv data/soc-pokec-relationships_matching.csv

spark-submit --master "local[*]" --class "final_project.verifier" target/scala-2.12/project_3_2.12-1.0.jar data/soc-pokec-relationships.csv data/soc-pokec-relationships_matching.csv
The matched edges form a matching of size: 644194




spark-submit --class "final_project.main" --driver-memory 12g  --master "local[*]" target/scala-2.12/project_3_2.12-1.0.jar compute  data/soc-LiveJournal1.csv data/soc-LiveJournal1_matching.csv

spark-submit --master "local[*]" --class "final_project.verifier" target/scala-2.12/project_3_2.12-1.0.jar data/soc-LiveJournal1.csv data/soc-LiveJournal1_matching.csv




spark-submit --class "final_project.main" --driver-memory 12g --master "local[*]" target/scala-2.12/project_3_2.12-1.0.jar compute  data/twitter_original_edges.csv data/twitter_original_edges_matching.csv

spark-submit --master "local[*]" --class "final_project.verifier" target/scala-2.12/project_3_2.12-1.0.jar data/twitter_original_edges.csv data/twitter_original_edges_matching.csv




spark-submit --class "final_project.main" --driver-memory 12g  --master "local[*]" target/scala-2.12/project_3_2.12-1.0.jar compute  data/com-orkut.ungraph.csv data/com-orkut.ungraph.csv

spark-submit --master "local[*]" --class "final_project.verifier" target/scala-2.12/project_3_2.12-1.0.jar data/com-orkut.

*/