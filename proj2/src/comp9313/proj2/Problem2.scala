/*
 * COMP9313 Big Data 18s2 Assignment 2
 * Problem2: Number of Cycles of Given Length
 * @Author: Tianpeng Chen
 * @zid: 5176343
 * Reference: < Distributed cycle detection in large-scale sparse graphs >
 * 						by R. C. Rocha and B. D. Thatte
 * 						(link: http://cdsid.org.br/sbpo2015/wp-content/uploads/2015/08/142825.pdf)
 */

package comp9313.proj2

import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks._

object Problem2 {
  def main(args: Array[String]) = {
    val conf = new SparkConf().setAppName("SSSP").setMaster("local")
    val sc = new SparkContext(conf)
    
    val fileName = args(0)
    val cycleLength = args(1).toInt
    val edges = sc.textFile(fileName)
    // read graph, each line in format (edgeId, srcNodeId, dstNodeId)
    // map that into (srcId, dstId, hasEdge: Boolean)
    val edgelist = edges.map(_.split(" "))
                        .map( x => Edge(x(1).toLong, x(2).toLong, true))
    val graph = Graph.fromEdges[Double, Boolean](edgelist, 0.0)
//    graph.triplets.collect().foreach(println)
    
    val initialGraph = graph.mapVertices( (id, _) => Array[Array[VertexId]]())
//    initialGraph.triplets.map( x => x.srcAttr.mkString("->") + " " + x.dstAttr.mkString("->")).collect().foreach(println)
    val initialMsg = Array(Array(-1.toLong))
    // define functions for pregel
    def vprog(vertexId: VertexId, neighbours: Array[Array[VertexId]], msg: Array[Array[VertexId]]
        ): Array[Array[VertexId]] = {
      // for each msg it receives, append itself to the end of each msg array
      // and set it as new attribute
      if (msg == initialMsg) {
        return Array(Array(vertexId))
      }

//      val new_neighbours : Array[Array[VertexId]] = 
//          msg.filter(!_.contains(vertexId))
//             .map(
//                x => x ++ Array(vertexId)
//      )
//      val new_neighbours : Array[Array[VertexId]] = 
//          msg.map(
//                x => x ++ Array(vertexId)
//      )
//      val new_neighbours = ArrayBuffer[Array[VertexId]]()
//      for (m <- msg) {
//        if (vertexId == m(0) && vertexId == m.min) {
//          println("Cycle detected: " + vertexId + "-->" + m.mkString("->"))
//          
//        } else if (!m.contains(vertexId)) {
//          println("Inner scope: " + new_neighbours.toArray.foreach(x=> println(x.mkString("->"))))
//          new_neighbours += (m ++ Array(vertexId))
//        }
//      }
      val new_neighbours = 
        for { m <- msg
              if (!m.contains(vertexId))
        } yield (m ++ Array(vertexId))
      
//      println("length of nb: " +  new_neighbours.length)
//      println("===== Outer scope =====")
//      println(new_neighbours.filter(!_.isEmpty).foreach(x => println(x.mkString("->"))))
//      println("========= End =========")
      return new_neighbours
//      val new_neighbours = Array[Array[VertexId]]()
//      for (m : Array[VertexId] <- msg) {
//        m.foreach(println)
//      }
    }
    
    def sendMsg(triplet: EdgeTriplet[Array[Array[VertexId]], Boolean]
        ): Iterator[(VertexId, Array[Array[VertexId]])] = {
      // deactivate vertex if
      // 1. no msg received
      // 2. vid is not the smallest in the current neighbour (the cycle should have been detected earlier)
      // cycle found if vid is the first in its attr(neighbours[neighbour])
      // and is also the smallest in that
      if (triplet.srcAttr.isEmpty) {
        return Iterator.empty
      }
//      val newMsg = ArrayBuffer[Array[VertexId]]()
//      for (attr <- triplet.srcAttr) {
//        if (triplet.srcId == attr(0) && triplet.srcId == attr.min && attr.length == cycleLength) {
//          println("Cycle detected: " + triplet.srcId + "-->" + attr.mkString("->"))
//        } else {
//          newMsg += attr
//        }
//      }
//      
//      return Iterator( (triplet.dstId, newMsg.toArray) )
      return Iterator( (triplet.dstId, triplet.srcAttr) )
    }
    
    def mergeMsg(a: Array[Array[VertexId]], b: Array[Array[VertexId]]): Array[Array[VertexId]] = {
      return a ++ b
    }
    
    // find all paths not containing a cycle of length (k - 1)
    val kPaths = initialGraph.pregel(
                  initialMsg, cycleLength - 1)(
                  vprog, sendMsg, mergeMsg)//.vertices.collect()
//    for (i <- kPaths.vertices.collect()){
//      for (j <- i._2){
//        print(i._1 + ": ")
//        println(j.mkString("->"))
//      }
//    }
    // find all cycles of length k
    
//    for (kPath <- kPaths) {
//      for (path <- kPath._2) {
//        if (kPath._1 == path(cycleLength - 1) && kPath._1 == path.min && kPath._1) {
//          result += (Array(kPath._1) ++ path)
//        }
//      }
//    }
//    var resultBuffer = ArrayBuffer[Array[VertexId]]()
//    val resultSet = collection.mutable.Set[Array[VertexId]]()
//    var cycleCount = 0
    
//    for (triplet <- kPaths.triplets) {
//      for (attr <- triplet.srcAttr) {
//        println("id: " + triplet.srcId + "\tattr: " + attr.mkString(" "))
//        if (triplet.srcId == attr(cycleLength - 1) && triplet.srcId == attr.min && triplet.dstId == attr(0) && triplet.attr) {
//          println("cycle detected")
//          println(triplet.srcId + "->" + attr.mkString("->"))
//          resultBuffer += ( Array(triplet.srcId) ++ attr.clone )
//          resultSet += ( Array(triplet.srcId) ++ attr.clone )
//          cycleCount += 1
//          
////          println(result.length)
////          resultBuffer.foreach(println)
//        }
////        println("2nd for loop")
////        resultBuffer.foreach(println)
//      }
      // finally, find cycle with length k
      val result = 
        for {  triplet <- kPaths.triplets
               attr <- triplet.srcAttr
               if (triplet.srcId == attr(cycleLength - 1) && triplet.srcId == attr.min && triplet.dstId == attr(0) && triplet.attr)
            } yield ( Array(triplet.srcId) ++ attr.clone )
          
        
//    println(cycleCount)
//    val result = resultBuffer.toArray
//    println(resultBuffer.length)
            
//    result.collect().foreach( x=> println(x.mkString("->")) )
    println(result.collect().length)
    
//    for (r <- result) {
//      println(r.mkString("->"))
//    }
//    result.foreach(println)
//    println(result.length)
//    println("%%%%%%%%%%%%%%%")
//    println(resultSet.foreach { x => println(x.mkString("->")) })
//    
  }
}