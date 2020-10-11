package com.zjl.spark.core.rdd.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 */
object Spark_action01 {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File_RDD")
    //创建Spark上下文环境对象（连接对象）
    val sc: SparkContext = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 6, 3, 5,4))

    //reduce
//    val res: Int = rdd.reduce(_ + _)
//    println(res)

    //collect
//    val ints: Array[Int] = rdd.collect()
//    ints.foreach(println)

    //count 获取RDD中元素的个数
//    val res: Long = rdd.count()
//    println(res)

    //first 返回RDD中的第一个元素
//    val res: Int = rdd.first()
//    println(res)

    //take 返回rdd前n个元素组成的数组
//    val ints: Array[Int] = rdd.take(2)
//    println(ints.mkString(","))

    //takeOrdered 获取RDD排序后 前n的元素组成的数组
    val ints: Array[Int] = rdd.takeOrdered(3)
    println(ints.mkString(","))



    //关闭连接
    sc.stop()
  }

}
