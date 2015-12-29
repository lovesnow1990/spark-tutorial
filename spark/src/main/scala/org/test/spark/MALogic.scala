package org.test.spark

class MALogic(close:Array[Double]) {
  def MA(n:Int):Array[Double] = {
    val rows = close.length
    var result = new Array[Double](rows)
    for (i <- n-1 until rows) {
      var sum = 0.0
      for ( j <- i-n+1 to i) {
        sum += close(j)
      }
      result(i) = sum/n
    }
    result
  }
}