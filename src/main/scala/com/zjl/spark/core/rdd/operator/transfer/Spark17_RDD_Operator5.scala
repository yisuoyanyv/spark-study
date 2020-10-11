package com.zjl.spark.core.rdd.operator.transfer

import org.apache.spark.{SparkConf, SparkContext}

object Spark17_RDD_Operator5 {
  def main(args: Array[String]): Unit = {
    //TODO Spark - RDD -算子（方法）


    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File_RDD")
    //创建Spark上下文环境对象（连接对象）
    val sc: SparkContext = new SparkContext(sparkConf)
    val dataRDD = sc.makeRDD(List(
      List(1, 2), List(3, 4)
    ))

    val rdd = dataRDD.flatMap(list => list)
    println(rdd.collect().mkString(","))
    sc.stop()


  }
}
