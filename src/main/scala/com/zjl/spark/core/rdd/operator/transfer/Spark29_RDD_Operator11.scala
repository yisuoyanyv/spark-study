package com.zjl.spark.core.rdd.operator.transfer

import org.apache.spark.{SparkConf, SparkContext}

object Spark29_RDD_Operator11 {
  def main(args: Array[String]): Unit = {
    //TODO Spark - RDD -算子（方法）

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File_RDD")
    //创建Spark上下文环境对象（连接对象）
    val sc: SparkContext = new SparkContext(sparkConf)

    val dataRDD = sc.makeRDD(List(1, 1, 1,4, 4,  4), 6)
//    val filterRDD = dataRDD.filter(num => num % 2 == 0)

    //TODO 当数据过滤后，发现数据不够均匀，那么可以缩减分区
//    val coalesceRDD=filterRDD.coalesce(1)
//    coalesceRDD.saveAsTextFile("output")

    //TODO 如果发现数据分区不合理，也可以缩减分区
    val coalesceRDD = dataRDD.coalesce(2)
    coalesceRDD.saveAsTextFile("output")
    sc.stop()


  }
}
