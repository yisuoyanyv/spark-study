package com.zjl.spark.core.rdd.operator.transfer

import org.apache.spark.{SparkConf, SparkContext}

object Spark24_RDD_Test {
  def main(args: Array[String]): Unit = {
    //将List(List(1,2),3,List(4,5))进行扁平化操作


    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File_RDD")
    //创建Spark上下文环境对象（连接对象）
    val sc: SparkContext = new SparkContext(sparkConf)

    val fileRDD = sc.textFile("input/apache.log")
    val timeRDD = fileRDD.map(
      log => {
        val datas = log.split(" ")
        datas(3)
      }
    )
    val hourRDD = timeRDD.groupBy(
      time => {
        time.substring(11, 13)
      }
    )
    sc.stop()


  }
}
