package org.test.spark

class RSILogic(close:Array[Double]) {
  private val rows = close.length
  private var posi = new Array[Double](rows)
  private var nega = new Array[Double](rows)
  for (i <- 1 until rows) {
    val dif = close(i)-close(i-1)
    if ( dif > 0) {
      posi(i) = dif
      nega(i) = 0.0
    }
    else {
      posi(i) = 0.0
      nega(i) = -dif
    }
  }
  def RSI(n:Int):Array[Double] = {
    var result = new Array[Double](rows)
      for (i <- n-1 until rows) {
        var up = 0.0
        var down = 0.0
        for (j <- i-n+1 to i) {
          up += posi(j)
          down += posi(j) + nega(i) 
        }
        if (down == 0) result(i) = 50.0
        else result(i) = up/down*100
      }
    result
  }
}