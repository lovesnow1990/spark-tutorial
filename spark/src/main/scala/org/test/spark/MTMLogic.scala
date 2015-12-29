package org.test.spark

class MTMLogic(close:Array[Double]) {
  def MTM(n:Int):Array[Double] = {
    val rows = close.length
    var result = new Array[Double](rows)
      for (i <- n until rows) {
        result(i) = close(i)-close(i-n)
      }
    result
  }
}