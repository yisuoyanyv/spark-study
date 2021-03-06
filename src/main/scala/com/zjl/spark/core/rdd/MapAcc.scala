package com.zjl.spark.core.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}


object MyAcc {
  def main(args: Array[String]): Unit = {
    //TODO Spark - 累加器

    //创建Spark运行配置对象
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File_RDD")

    //创建Spark上下文环境对象（连接对象）
    val sc: SparkContext = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4,5,6),2)


    //先注册自定义的累加器
    val acc = new MapAcc
    sc.register(acc,"first")

   rdd.foreach(x=>acc.add(x))
    println(acc.value)

    sc.stop()


  }
}

//将来累加器的值同时包含sum,count，avg
//(sum,count,avg)
//Map("sum"->1000,"count"->10,"avg"->100)
class MapAcc extends AccumulatorV2[Double,Map[String,Any]] {
  private var map: Map[String, Any] = Map[String, Any]()

  override def isZero: Boolean = map.isEmpty

  override def copy(): AccumulatorV2[Double, Map[String, Any]] = {
    val acc:MapAcc=new MapAcc
    acc.map = map
    acc
  }

  //不可变集合，直接赋值一个空的集合
  override def reset(): Unit = map=Map[String,Any]()

  override def add(v: Double): Unit = {
    //分区内
    //对sum和count进行累加，avg在最后value函数进行计算
    map+="sum" -> (map.getOrElse("sum",0D).asInstanceOf[Double]+v)
    map+="count" -> (map.getOrElse("count",0L).asInstanceOf[Long]+1)
  }

  //分区间
  override def merge(other: AccumulatorV2[Double, Map[String, Any]]): Unit = {
    //合并两个map
    other match {
      case o:MapAcc=>
        map+="sum"->(map.getOrElse("sum",0D).asInstanceOf[Double]+o.map.getOrElse("sum",0D).asInstanceOf[Double])
        map+="count"->(map.getOrElse("count",0L).asInstanceOf[Long]+o.map.getOrElse("count",0L).asInstanceOf[Long])
      case _=>throw new UnsupportedOperationException
    }
  }

  override def value: Map[String, Any] = {
    map+="avg"->(map.getOrElse("sum",0D).asInstanceOf[Double]/map.getOrElse("count",0L).asInstanceOf[Long])
    map
  }
}