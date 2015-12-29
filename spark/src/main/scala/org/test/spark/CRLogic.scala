package org.test.spark

class CRLogic(high:Array[Double],low:Array[Double]) {
  def CR(n:Int):Array[Double]= {
    val rows = high.length
    var result = new Array[Double](rows)
    var up = new Array[Double](rows)
    var down = new Array[Double](rows)
    var center = new Array[Double](rows)
    center(0) = (high(0)+low(0))/2
    for (i <- 1 until rows) {
      center(i) = (high(i)+low(i))/2
      up(i) = high(i) - center(i-1)
      down(i) = center(i-1) - low(i)
    }
    for (i <- n-1 until rows) {
      var sum_up = 0.0
      var sum_down = 0.0
      for (j <- i-n+1 to i) {
        sum_up += up(j)
        sum_down += down(j)
      }
      if (sum_down == 0) {
        result(i) = -999.0
      }
      else if (sum_up < 0 || sum_down < 0) {
        result(i) = 0.0
      }
      else
        result(i) = sum_up/sum_down*100
    }
    result
  }
}