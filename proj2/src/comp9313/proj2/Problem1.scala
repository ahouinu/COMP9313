/*
 * COMP9313 Big Data 18s2 Assignment 2
 * Problem1: Relative Term Frequency
 * @Author: Tianpeng Chen
 * @zid: 5176343
 * Reference: Chapter 5, << Data Algorithms: Recipes for Scaling up with Hadoop and Spark >> 
 * 						by Mahmoud Parsian (mahmoud.parsian@yahoo.com)
 * 						(link: https://github.com/mahmoudparsian/data-algorithms-book/tree/master/src/main/scala/org/dataalgorithms)
 */

package comp9313.proj2

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object Problem1 {
  def main(args: Array[String]) {
    val inputFile = args(0)
    val outputFolder = args(1)
    val conf = new SparkConf().setAppName("Problem1").setMaster("local")
    val sc = new SparkContext(conf)
    val input = sc.textFile(inputFile)
//    val tokens = input
//                .flatMap(_.toLowerCase()
//                          .split("[\\s*$&#/\"'\\,.:;?!\\[\\](){}<>~\\-_]+"))
    val _sep = "[\\s*$&#/\"'\\,.:;?!\\[\\](){}<>~\\-_]+"
    /*
     * map raw input into the format:
     * ( term1, (term2, 1) )
     */
    val pairs = input.flatMap(line => {
      val tokens = line.toLowerCase()
                       .split(_sep)
                       .filter( x => (x.nonEmpty && x.charAt(0) >='a' && x.charAt(0) <= 'z' ) )
      for {
        i <- 0 until tokens.length
        start = i + 1
        end = tokens.length - 1
        j <- start to end if (j != i)
      } yield (tokens(i), (tokens(j), 1))
    })
    /*
     * for each term, count total number of terms after it in the same line
     * ( term1, int )
     */
    val totalNumberByKey = pairs.map( x => (x._1, x._2._2) )
                                .reduceByKey(_+_)
    // ( term1, [ (term2, int), ... ] )
    val groupedPairs = pairs.groupByKey()
                            .flatMapValues(_.groupBy(_._1).mapValues(_.unzip._2.sum))
    
    // ( term1, {( term2, sum(term2) ), sum(term1)} )
    val joined = groupedPairs join totalNumberByKey
    /*
     * calculate co-relative freq for each key
     * ( (term1, term2), sum(term2)/sum(term1) )
     */
    val relativeFreq = joined.map( t => {
      ((t._1, t._2._1._1), (t._2._1._2.toDouble / t._2._2.toDouble))
    })
    // sort by term1 ascending, then by freq descending
    // then convert it into Array(String) to fit the output format
    val sortedRelFreq = relativeFreq.sortBy( x => (x._1._1, -x._2, x._1._2))
//    val sortedRelFreq = relativeFreq.sortBy( x => (x._1._1, x._1._2, -x._2))
                                    .map( x => (x._1._1 + " " + x._1._2 + " " + x._2.toString) )
    sortedRelFreq.saveAsTextFile(outputFolder)
   }
}