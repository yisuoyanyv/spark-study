package com.zjl.spark.core.rdd.operator.transfer

import org.apache.spark.{SparkConf, SparkContext}

object Spark28_RDD_Operator10 {
  def main(args: Array[String]): Unit = {
    //TODO Spark - RDD -算子（方法）

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File_RDD")
    //创建Spark上下文环境对象（连接对象）
    val sc: SparkContext = new SparkContext(sparkConf)

    val dataRDD = sc.makeRDD(List(1, 2, 3, 3, 5, 6), 3)
    //TODO distinct 去重
    val rdd1 = dataRDD.distinct()
    //distinct 可以改变分区的数量
    val rdd2 = dataRDD.distinct(2)
    println(rdd1.collect().mkString(","))
    println(rdd2.collect().mkString(","))

    sc.stop()


  }
}
