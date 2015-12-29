package org.test.spark

class KDLogic(close:Array[Double], high:Array[Double], low:Array[Double]) {
  def RSV(n:Int):Array[Double] = {
    val rows = close.length
    var rsv = new Array[Double](rows)
    for (i <- n-1 until rows) {
      var max = -99999.0
      var min = 99999.0
      for (j <-i-n+1 to i) {
        max = if(max > high(j)) max else high(j)
        min = if(min < low(j)) min else low(j)
      }
      rsv(i) = (close(i)-min)/(max-min)*100
    }
    rsv
  }
  def KDK(n:Int):Array[Double] = {
    val rows = close.length
    var kd_k = new Array[Double](rows)
    val RS = RSV(n)
    kd_k(n-1) = 50.0
    for ( i <- n until rows) {
      kd_k(i) = (1.0-1.0/n) * kd_k(i-1)+(1.0/n)*RS(i)
    }
    kd_k
  }
  def KDD(n:Int):Array[Double] = {
    val rows = close.length
    var result = new Array[Double](rows)
    val K = KDK(n)
    result(n-1) = 50.0
    for (i <- n until rows) {
      result(i) = (1.0-1.0/n)*result(i-1)+(1.0/n)*K(i)
    }
    result
  }
}