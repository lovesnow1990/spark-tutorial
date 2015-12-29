package org.test.spark

class BIASLogic(close:Array[Double]) {
  def BIAS(n:Int):Array[Double] = {
    val rows = close.length
    var result = new Array[Double](rows)
    val ma = new MALogic(close)
    val data = ma.MA(n)
    for (i <- n-1 until rows) {
      result(i) = (close(i)-data(i))/data(i)*100
    }
    result
  }
}