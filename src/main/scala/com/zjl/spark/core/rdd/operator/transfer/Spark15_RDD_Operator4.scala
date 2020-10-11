package com.zjl.spark.core.rdd.operator.transfer

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark15_RDD_Operator4 {
  def main(args: Array[String]): Unit = {
    //TODO Spark - RDD -算子（方法）


    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File_RDD")
    //创建Spark上下文环境对象（连接对象）
    val sc: SparkContext = new SparkContext(sparkConf)

    //TODO 获取每个分区最大值以及分区号
    val dataRDD: RDD[Int] = sc.makeRDD(List(1, 4, 3, 2, 5, 6), 3)
    val rdd = dataRDD.mapPartitionsWithIndex(
      (index, iter) => {
        List((index, iter.max)).iterator
      }
    )
    println(rdd.collect().mkString(","))

    sc.stop()


  }
}
