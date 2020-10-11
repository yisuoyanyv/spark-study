package com.zjl.spark.core.rdd.operator.transfer

import org.apache.spark.{SparkConf, SparkContext}

object Spark18_RDD_Test {
  def main(args: Array[String]): Unit = {
    //将List(List(1,2),3,List(4,5))进行扁平化操作


    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File_RDD")
    //创建Spark上下文环境对象（连接对象）
    val sc: SparkContext = new SparkContext(sparkConf)

    //获取第二个数据分区的数据
    val dataRDD = sc.makeRDD(List(List(1, 2), 3, List(4, 5)))
    val rdd = dataRDD.flatMap(
      data => {
        data match {
          case list: List[_] => list
          case d => List(d)
        }
      }
    )
    println(rdd.collect().mkString(","))


    sc.stop()


  }
}
