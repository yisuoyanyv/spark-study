package com.zjl.data.mock.util

import scala.collection.mutable.ListBuffer

/**
 * 根据提供的值和比较，来创建RandomOptions对象
 * 然后可以通过getRandomOption来获取一个随机的预定义的值
 */
object RandomOptions {
  def apply[T](opts:(T,Int)*):RandomOptions[T]={
    val randomOptions=new RandomOptions[T]()
    randomOptions.totalWeight=(0 /: opts)(_ + _._2)  //计算出来总的比重
    opts.foreach{
      case (value,weight)=>randomOptions.options ++= (1 to weight).map(_ => value)
    }
    randomOptions
  }

  def main(args: Array[String]): Unit = {
    //测试
    val opts=RandomOptions(("张三",10),("李四",30),("ww",20))

    println(opts.getRandomOption())
    println(opts.getRandomOption())
  }

}

class RandomOptions[T]{
  var totalWeight:Int =_
  var options:ListBuffer[T]=ListBuffer[T]()

  def getRandomOption():T={
    options(RandomNumberUtil.randomInt(0,totalWeight-1))
  }

}
