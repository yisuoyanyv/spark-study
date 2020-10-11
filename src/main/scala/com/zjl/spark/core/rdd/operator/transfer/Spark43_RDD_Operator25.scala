package com.zjl.spark.core.rdd.operator.transfer

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark43_RDD_Operator25 {
  def main(args: Array[String]): Unit = {
    //TODO Spark - RDD -算子（方法）

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File_RDD")
    //创建Spark上下文环境对象（连接对象）
    val sc: SparkContext = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 3, 2, 4),5)

    val result: Int = rdd.aggregate(0)(_ + _, _ + _)
    val result1: Int = rdd.aggregate(10)(_ + _, _ + _)



    println(result)
    println(result1)
    sc.stop()

  }

  
}
