package com.zjl.spark.core.rdd.operator.transfer

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark46_RDD_Operator28 {
  def main(args: Array[String]): Unit = {
    //TODO Spark - RDD -算子（方法）

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File_RDD")
    //创建Spark上下文环境对象（连接对象）
    val sc: SparkContext = new SparkContext(sparkConf)
    val rdd = sc.makeRDD(List(1,2,3,4))
    println(rdd.toDebugString)
    println(rdd.dependencies)

    rdd.map(num=>num).collect().foreach(println)

    println("******************")

    //分布式打印
    rdd.foreach(println)
    sc.stop()

  }

  
}
