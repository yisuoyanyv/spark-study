package com.zjl.spark.core.rdd.operator.transfer

import org.apache.spark.{SparkConf, SparkContext}

object Spark31_RDD_Operator13 {
  def main(args: Array[String]): Unit = {
    //TODO Spark - RDD -算子（方法）

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File_RDD")
    //创建Spark上下文环境对象（连接对象）
    val sc: SparkContext = new SparkContext(sparkConf)

    val dataRDD = sc.makeRDD(List(1, 1, 1,4, 4,  4), 3)

    //缩减分区：coalesce
    val coalesceRDD = dataRDD.coalesce(2)
    //TODO 扩大分区：repartition
    //从底层源码的角度。repartition其实就是coalesce，并且肯定进行shuffle操作
    val repartionRDD =dataRDD.repartition(6)

    sc.stop()


  }
}
