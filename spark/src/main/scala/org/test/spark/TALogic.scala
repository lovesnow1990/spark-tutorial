package org.test.spark

class TALogic(open:Array[Double], high:Array[Double], low:Array[Double], close:Array[Double]) {
  val rows = open.length
  def AR(n:Int):Array[Any] = {
    var AR = new Array[Any](rows)
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
      if (down == 0) AR(i) = null
      else AR(i) = up/down
    }
    AR
  }
  
  def BIAS(n:Int):Array[Double] = {
    var result = new Array[Double](rows)
    val data = MA(n)
    for (i <- n-1 until rows) {
      result(i) = (close(i)-data(i))/data(i)*100
    }
    result
  }
  
  def CR(n:Int):Array[Any]= {
    var result = new Array[Any](rows)
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
        result(i) = null
      }
      else if (sum_up < 0 || sum_down < 0) {
        result(i) = 0.0
      }
      else
        result(i) = sum_up/sum_down*100
    }
    result
  }
  
  def EMA(n:Int):Array[Double] = {
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
  
  def RSV(n:Int):Array[Double] = {
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
    var kd_k = new Array[Double](rows)
    val RS = RSV(n)
    kd_k(n-1) = 50.0
    for ( i <- n until rows) {
      kd_k(i) = (1.0-1.0/n) * kd_k(i-1)+(1.0/n)*RS(i)
    }
    kd_k
  }
  def KDD(n:Int):Array[Double] = {
    var result = new Array[Double](rows)
    val K = KDK(n)
    result(n-1) = 50.0
    for (i <- n until rows) {
      result(i) = (1.0-1.0/n)*result(i-1)+(1.0/n)*K(i)
    }
    result
  }
  
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
  
  def MTM(n:Int):Array[Double] = {
    var result = new Array[Double](rows)
      for (i <- n until rows) {
        result(i) = close(i)-close(i-n)
      }
    result
  }
  
  def OSC(n:Int):Array[Double] = {
    var result = new Array[Double](rows)
      for (i <- n until rows) {
        result(i) = close(i)/close(i-n)*100
      }
    result
  }
  
  private var posi = new Array[Double](rows)
  private var nega = new Array[Double](rows)
  for (i <- 1 until rows) {
    val dif = close(i)-close(i-1)
    posi(i) = if (dif > 0) dif else 0.0
    nega(i) = if (dif > 0) 0.0 else -dif
  }
  def RSI(n:Int):Array[Double] = {
    var result = new Array[Double](rows)
      for (i <- n-1 until rows) {
        var up = 0.0
        var down = 0.0
        for (j <- i-n+1 to i) {
          up += posi(j)
          down += posi(j) + nega(j) 
        }
        result(i) = if (down == 0) 50.0 else up/down*100
      }
    result
  }
  
  def WMSR(n:Int):Array[Double] = {
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