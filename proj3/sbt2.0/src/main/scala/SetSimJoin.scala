/*
 * COMP9313 Big Data 18s2 Assignment 3
 * Set Similarity Join
 * @Author: Tianpeng Chen
 * @zid: 5176343
 * Reference:
 * 1. C. Xiao, W. Wang, X. Lin, J. X. Yu. Efficient similarity joins for near duplicate detection
 * 		https://www.cse.unsw.edu.au/~lxue/WWW08.pdfd
 * 2. L. A. Ribeiro, T. Harder. Generalizing prefix filtering to improve set similarity joins
 * 		https://www.lgis.informatik.uni-kl.de/cms/fileadmin/publications/2010/RH10.IS.pdf
 * 3. Tuning Spark
 * 		https://spark.apache.org/docs/latest/tuning.html
 */

package comp9313.proj3

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.collection.mutable.HashMap
import scala.math.ceil
import org.apache.spark.storage.StorageLevel

//import scala.math.BigDecimal

object SetSimJoin {
  def main(args: Array[String]) {
    val inputFile1 = args(0)
    val inputFile2 = args(1)
    val outputFolder = args(2)
    val simThreshold = args(3).toDouble
    val conf = new SparkConf().setAppName("SetSimJoin").setMaster("local")
//    val conf = new SparkConf().setAppName("SetSimJoin")
    val sc = new SparkContext(conf)
    
    // read input files
    val input1 = sc.textFile(inputFile1).map(_.split(" "))
    val input2 = sc.textFile(inputFile2).map(_.split(" "))
    
    // union 2 input RDDs into 1
    // val inputAll = input1.union(input2)
    val inputAll = input1++input2
    
    /*
     * Stage 1: order tokens by frequency (Canonicalization)
     */
    
    // map input into [ (token, freq) ]
    val tokenFreq = inputAll.map( x => x.drop(1) )
//                            .flatMap(_.split(" "))
                            .flatMap( x => for{ i <- x } yield (i, 1))
                            .reduceByKey(_+_)
    // sort pairs w.r.t frequency (NO NEED)
//    val sortedTokenFreq = tokenFreq.sortBy( x => (x._2, x._1))
    
    // convert sorted (token, freq) pairs to a HashMap, and then save that as a broadcast variable
//    val orderMap = sortedTokenFreq.collectAsMap()
    val orderMap = tokenFreq.collectAsMap()
    val broadcastOrder = sc.broadcast(orderMap)
    
    /*
     * Stage 2: find 'similar' id pairs
     */
    
    // map input1 and input2 to [ (rid, token) ]
    val pair1 = input1.map( x => (x(0).toLong, x.slice(1, x.length)))
    val pair2 = input2.map( x => (x(0).toLong, x.slice(1, x.length)))
                     
    // sort tokens for each document w.r.t orderMap (from broadcast)
    val sortedPair1 = pair1.map( x => (x._1, x._2.sortBy( c => (broadcastOrder.value.get(c), c))))
    val sortedPair2 = pair2.map( x => (x._1, x._2.sortBy( c => (broadcastOrder.value.get(c), c))))
    
    // check sorted result, OK
//    sortedPair1.collect().foreach( x => {
//      println(x._1 + "->" + x._2.mkString(" "))
//    })
    
    // Calculate maxpref size for every pair
    // for doc x, |maxpref| = |x| - ceiling(minsize(x)) + 1
    // where minsize(x) = t|x|
    // i.e. |maxpref| = |x| - ceiling(t*|x|) + 1
    
//    val maxprefSize1 = sortedPair1.map( x => (x._1, x._2.length - ceil(x._2.length * simThreshold) + 1))
//    val docLenPair = pair1.map( x => (x._1, x._2.length))
    // compute maxpref size for pair1 and pair2, in format [ (rid, maxpref size) ]
//    val maxprefSize1 = pair1.map( x => (x._1, x._2.length))
//                            .map( x => (x._1, (x._2 - ceil(x._2 * simThreshold) + 1).toInt))
//    val maxprefSize2 = pair2.map( x => (x._1, x._2.length))
//                            .map( x => (x._1, (x._2 - ceil(x._2 * simThreshold) + 1).toInt))
//    maxprefSize1.foreach(println)
//    maxprefSize2.foreach(println)
    // map pairs into (rid, tokens, length), then into (rid, tokens, maxprefSize)
//    val maxprefSize1 = sortedPair1//.map( x => (x._1, x._2, x._2.length))
//                                  .map( x => (x._1, x._2, (x._2.length - ceil(x._2.length * simThreshold) + 1).toInt))
//    val maxprefSize2 = sortedPair2//.map( x => (x._1, x._2, x._2.length))
//                                  .map( x => (x._1, x._2, (x._2.length - ceil(x._2.length * simThreshold) + 1).toInt))
    // check, OK
//    maxprefSize1.foreach(x => println(x._1 + ", " + x._2.mkString(" ") + ", " + x._3))
    
    // then into (rid, tokens, maxpref)
//    val maxpref1 = maxprefSize1.map( x => (x._1, x._2, x._2.slice(0, x._3)))
//    val maxpref2 = maxprefSize2.map( x => (x._1, x._2, x._2.slice(0, x._3)))
//      val maxpref1 = sortedPair1.map( x => {
//        (x._1, x._2, x._2.dropRight(ceil(x._2.length * simThreshold).toInt))
//      })
//      val maxpref2 = sortedPair2.map( x => {
//        (x._1, x._2, x._2.dropRight(ceil(x._2.length * simThreshold).toInt))
//      })
      val revPair1 = sortedPair1.map( x => {
        (x._2.dropRight(ceil(x._2.length * simThreshold).toInt - 1), ((x._1, 'R'), x._2))
      })
      val revPair2 = sortedPair2.map( x => {
        (x._2.dropRight(ceil(x._2.length * simThreshold).toInt - 1), ((x._1, 'S'), x._2))
      })
    // check, OK
//    maxpref1.foreach(x => println(x._1 + ", (" + x._2.mkString(" ") + "), (" + x._3.mkString(" ") + ")"))
//    maxpref2.foreach(x => println(x._1 + ", (" + x._2.mkString(" ") + "), (" + x._3.mkString(" ") + ")"))
    // take maxpref as key, i.e. convert pairs into (maxpref, ( (rid, set), tokens))
//    val revPair1 = maxpref1.map(x => (x._3, ( (x._1, "R"),  x._2)))
//    val revPair2 = maxpref2.map(x => (x._3, ( (x._1, "S"), x._2)))
    // flatten
//    val flatPair1 = for {
//      pair <- revPair1;
//      key <- pair._1
//    } yield (key, pair._2)
//    val flatPair2 = for {
//      pair <- revPair2;
//      key <- pair._1
//    } yield (key, pair._2)
    val flatPair1 = revPair1.flatMap( x => for {
      prefix <- x._1
    } yield (prefix, x._2))
    val flatPair2 = revPair2.flatMap( x => for {
      prefix <- x._1
    } yield (prefix, x._2))
    
    // check, OK
//    flatPair1.foreach(x => println("key: " + x._1 + "\trid: " + x._2._1 + "\ttokens: " + x._2._2.mkString(" ")))
//    flatPair2.foreach(x => println("key: " + x._1 + "\trid: " + x._2._1 + "\ttokens: " + x._2._2.mkString(" ")))
    
    // group by key (same prefix char)
//    val groupedPair1 = flatPair1.groupByKey()
//    val groupedPair2 = flatPair2.groupByKey()
    
    // combine two sets
    val flatPair = flatPair1.union(flatPair2)
//    flatPair.foreach(x => println("key: " + x._1 + "\trid: " + x._2._1 + "\ttokens: " + x._2._2.mkString(" ")))
    val groupedPair = flatPair.groupByKey()
    
    // check, sample output is:
    // example 1:
    //  key: 4	values: 
    //  rid: (0,R)	tokens: 1 4 5 6
    //  rid: (2,R)	tokens: 4 5 6
    //  rid: (0,S)	tokens: 1 4 6
//    groupedPair.foreach(x => {
//      println("key: " + x._1 + "\tvalues: ")
//      x._2.foreach(y => println("rid: " + y._1 + "\ttokens: " + y._2.mkString(" ")))
//      println("--------------------------")
//    })
    
    // for each key in grouped pairs, enumerate similar id pairs (with their tokens) from R and S respectively
    // format ((rid, rtokens), (sid, stokens))
    // output from example 1 should be like:
    // key: 4  values:
    // ((0, [1, 4, 5, 6]), (0, [1, 4, 6])
    // ((2, [1, 4, 5, 6]), (0, [1, 4, 6])
//    val idPairs = groupedPair.flatMap( x => {
// 
//      for {
//        // doc1._1._1 is the doc id, and doc1._1._2 is its set (R or S)
//        // doc1._2 is its tokens
//        doc1 <- x._2 if doc1._1._2 == "R";
//        doc2 <- x._2 if (doc2._1._2 == "S")
//        
//        // yield (rid, sid), (rtokens, stokens) pair
//    } yield ((doc1._1._1, doc2._1._1), (doc1._2, doc2._2))
//    // then group duplicates
//    }).reduceByKey((a, b) => a)
    val idPairs = groupedPair.flatMap( x => {
      val groupByOrigin = (x._2.groupBy( y => y._1._2 ))
//      println("Key: " + groupByOrigin._1)
//      groupByOrigin._2.foreach(y => y.foreach( z => println(z._1 + "\t" + z._2.mkString(", "))))
      //groupByOrigin
    //.filter( x => x._2.size > 1)
      //.mapPartitions(line => line
      //.flatMap( x =>
      if (groupByOrigin.size != 2){
        Iterator.empty
      } else {
        val re = for {
          r <- groupByOrigin.apply('R');
          // filter sdoc within size [|r|*t, |r|/t]
          s <- groupByOrigin.apply('S') if (s._2.size.toDouble >= r._2.size.toDouble * simThreshold
                                        && s._2.size <= r._2.size.toDouble / simThreshold)
        // yield ( (rid, rtoken), (sid, stoken) )
        } yield ((r._1._1, s._1._1), (r._2, s._2))//)
      re
      }
    })
      // filter out duplicates
      .reduceByKey((a, b) => a)
//      idPairs.collect().foreach(println)
//    
//    idPairs.foreach( x => {
//      println("Key: " + x._1 + "\tSet: " + x._2.toString())
////      x._2.foreach(y => y.foreach( z => println(z._1 + "\t" + z._2.mkString(", "))))
//    })
      // then filter out empty pairs (No need for using flatMap)
//    }).filter(x => !x._2.isEmpty)
//    }).filter(!_.isEmpty)
      
    
    // check output
//    idPairs.foreach( x => {
//      println("key: " + x._1 + "\tvalues: ")
//      x._2.foreach(y => {
////        print("{(R, " + y._1._1 + "), tokens: (" + y._1._2.mkString(", ") + "), ")
////        println("(S, " + y._2._1 + "), tokens: (" + y._2._2.mkString(", ") + ")}")
//        println("{(" + y._1 + "\trtokens: (" + y._2._1.mkString(", ") + "), stokens: (" + y._2._2.mkString(", ") + ")}")
//            })
//      println("--------------------------")
//    })
//    idPairs.foreach( x => {
//      println(x._1)
//      x._2.foreach( x => {
//        println(x._1)
//        x._2.foreach({ y => 
//        println("{(" + y._1 + "\trtokens: (" + y._2.mkString(", "))// + "),\tstokens: (" + y._2._2.mkString(", ") + ")}")
//        })})
//    })
//    idPairs.foreach( x => { 
//      println("key: " + x._1)
//      x._2.foreach( y => {
//        println("rtokens: (" + y._1.mkString(", ") + "), \tstokens: (" + y._2.mkString(", ") + ")")
//      })
//    })
    
    
    // calculate JS for each key (for each pair of docs sharing more than one token in their prefixes)
    // which is already filtered out in the previous step
    // for doc r and s from R and S respectively, the Jaccard Similarity (JS) will be: 
    // JS(r, s) = ( |rns| / |rus|) = ( |rns| / (|r| + |s| - |rns|))
    
    // Now we have pairs in format like ( (rid, sid), (rtokens, stokens) )
    // {((2,2)	rtokens: (4, 5, 6),	stokens: (3, 5)}
    // calculate rns, rus, and then calculate JS
    val result = idPairs.map( x => {
      // alias make life easier
      val rid = x._1._1
      val sid = x._1._2
      val rset = x._2._1.toSet
      val sset = x._2._2.toSet
      val rsize = rset.size
      val ssize = sset.size
      val intersize = rset.intersect(sset).size
      val unionsize = rsize + ssize - intersize
//      val sim = BigDecimal(intersize) / BigDecimal(unionsize)
      val sim = intersize.toDouble / unionsize.toDouble
//      println("calculating sim(" + rid + ", " + sid + ")")
//      println("rsize: " + rsize)
//      println("ssize: " + ssize)
//      println("intersize: " + intersize)
//      println("unionsize: " + unionsize)
//      println("JS: " + sim)
      // yield
//      ((rid, sid), sim.round(new java.math.MathContext(6)))
      ((rid, sid), BigDecimal(sim).setScale(6, BigDecimal.RoundingMode.HALF_UP).toDouble)
      // filter out bad results
    }).filter(x => x._2 >= simThreshold)
//    .sortBy(x => (x._1._1, x._1._2))
      // sort 'em!
//      .sortBy(x => (x._1._1.toInt, x._1._2.toInt))
    
//    val sorted = result.persist(StorageLevel.MEMORY_AND_DISK)
//                .sort( x => {
//                  
//                }, 16)
//    val sorted = repartitionAndSortWithinPartitions(partitioner)
//    result.collect.foreach(println)
      // ready to output, make up string
      val output = result.sortBy(_._1)
          .map( x => 
                x._1.toString + "\t" + x._2.toString
              )
//        output.foreach(println)
//        println(output.collect().size)
      output.saveAsTextFile(outputFolder)
  }
}