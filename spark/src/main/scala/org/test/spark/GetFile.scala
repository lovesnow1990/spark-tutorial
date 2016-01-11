package org.test.spark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.io.Source

object GetFile {
  def main(args:Array[String]) {
    val conf = new SparkConf()
      .setAppName("TranTest")
      .setMaster("local")
    val sc = new SparkContext(conf)
    val data = Source.fromFile("D://FCT.txt").getLines.toArray
    val disdata = sc.parallelize(data)
    //val disdata2 = disdata.map(line => line.split(",").map(_.toDouble)).cache()
    val withIndex = disdata.zipWithIndex
    val indexKey = withIndex.map{case (k,v) => (v,k)}
    indexKey.foreach(println)
    //println(indexKey.lookup(0))
    //disdata2(0)
  }
}