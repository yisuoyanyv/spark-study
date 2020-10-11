package com.zjl.spark.core.rdd.operator.transfer

import org.apache.spark.{SparkConf, SparkContext}

object Spark25_RDD_Test {
  def main(args: Array[String]): Unit = {
    //将List(List(1,2),3,List(4,5))进行扁平化操作


    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File_RDD")
    //创建Spark上下文环境对象（连接对象）
    val sc: SparkContext = new SparkContext(sparkConf)
    val dataRDD = sc.makeRDD(List("Hello Word", "Hello Spring"))
    println(dataRDD
      .flatMap(_.split(" "))
      .groupBy(word => word)
      .map(kv => (kv._1, kv._2.size))
      .collect().mkString(","))

    sc.stop()


  }
}
