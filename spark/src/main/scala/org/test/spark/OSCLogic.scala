package org.test.spark

class OSCLogic(close:Array[Double] ){
  def OSC(n:Int):Array[Double] = {
    val rows = close.length
    var result = new Array[Double](rows)
      for (i <- n until rows) {
        result(i) = close(i)/close(i-n)*100
      }
    result
  }
}