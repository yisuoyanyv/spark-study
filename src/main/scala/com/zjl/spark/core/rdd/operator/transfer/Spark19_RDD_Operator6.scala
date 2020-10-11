package com.zjl.spark.core.rdd.operator.transfer

import org.apache.spark.{SparkConf, SparkContext}

object Spark19_RDD_Operator6 {
  def main(args: Array[String]): Unit = {
    //TODO Spark - RDD -算子（方法）

    //glom =>将每个分区的数据转换为数组
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File_RDD")
    //创建Spark上下文环境对象（连接对象）
    val sc: SparkContext = new SparkContext(sparkConf)

    val dataRDD = sc.makeRDD(List(1, 2, 3, 4), 2)

    val rdd = dataRDD.glom()

    rdd.foreach(
      array => {
        println(array.mkString(","))
      }
    )

    sc.stop()


  }
}
