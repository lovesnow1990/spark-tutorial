package org.test.spark

class ARLogic(open:Array[Double], high:Array[Double], low:Array[Double]){
  def AR(n:Int):Array[Double] = {
    val rows = open.length
    var AR = new Array[Double](rows)
    var O_L = new Array[Double](rows)
    var H_O = new Array[Double](rows)
    for (i <- 0 until rows) {
      O_L(i) = open(i)-low(i)
    }
    for (i <- 0 until rows) {
      H_O(i) = high(i) - open(i)
    }
    for ( i <- n-1 until rows) {
      var up = 0.0
      var down = 0.0
      for (j <- i-n+1 to i) {
          up += H_O(j)
          down += O_L(j)
      }
      if (down == 0) AR(i) = -999.0
      else AR(i) = up/down
    }
    AR
  }
}