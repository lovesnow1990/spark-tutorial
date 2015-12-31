package org.test.readFile

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.io.Source

object TranTest {
  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("TranTest")
      .setMaster("local")
    val sc = new SparkContext(conf)
    val data = Source.fromFile("D://FCT.txt").getLines.toArray
    val disdata = sc.parallelize(data)
    //disdata.foreach(println)
    
    //val rdd = sc.parallelize(Seq(Seq(1, 2, 3), Seq(4, 5, 6), Seq(7, 8, 9)))
    //rdd.foreach(println)
    
    
    // Split the matrix into one number per line.
    val byColumnAndRow = disdata.zipWithIndex.flatMap {
      case (row, rowIndex) => row.zipWithIndex.map {
        case (number, columnIndex) => columnIndex -> (rowIndex, number)
      }
    }
// Build up the transposed matrix. Group and sort by column index first.
    val byColumn = byColumnAndRow.groupByKey.sortByKey().values
// Then sort by row index.
    val transposed = byColumn.map {
      indexedRow => indexedRow.toSeq.sortBy(_._1).map(_._2)
    }
    println(transposed)
    println("finish")
  }
}