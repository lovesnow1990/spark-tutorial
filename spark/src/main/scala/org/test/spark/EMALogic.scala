package org.test.spark

class EMALogic(close:Array[Double]) {
  def EMA(n:Int):Array[Double] = {
    val rows = close.length
    val alpha = 2/(n+1)
    var result = new Array[Double](rows)
    for (i<-0 until n-1) {
      result(n-1) += close(i)
    }
    result(n-1) = result(n-1)/n
    for (i <- n until rows) {
      result(i) = alpha*close(i) + (1-alpha)*result(i-1)
    }
    result
  }
}