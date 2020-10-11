package com.zjl.spark.core.rdd.operator.transfer

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark42_RDD_Operator24 {
  def main(args: Array[String]): Unit = {
    //TODO Spark - RDD -算子（方法）

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File_RDD")
    //创建Spark上下文环境对象（连接对象）
    val sc: SparkContext = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 3, 2, 4))

    val countResult: Long = rdd.count()

    println(countResult)

    val takeResult: Array[Int] = rdd.take(2)
    takeResult.foreach(println)

    val result: Array[Int] = rdd.takeOrdered(2)

    println(result.mkString(","))
    sc.stop()

  }

  
}
