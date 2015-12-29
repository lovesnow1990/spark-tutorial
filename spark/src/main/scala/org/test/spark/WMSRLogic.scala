package org.test.spark

class WMSRLogic(close:Array[Double], high:Array[Double], low:Array[Double]) {
  def WMSR(n:Int):Array[Double] = {
    val rows = close.length
    var result = new Array[Double](rows)
      for (i <- n-1 until rows) {
        var max = -99999.0
        var min = 99999.0
        for (j <-i-n+1 to i) {
          max = if(max > high(j)) max else high(j)
          min = if(min < low(j)) min else low(j)
        }
        result(i) = (max-close(i))/(max-min)*100
      }
    result
  }
}