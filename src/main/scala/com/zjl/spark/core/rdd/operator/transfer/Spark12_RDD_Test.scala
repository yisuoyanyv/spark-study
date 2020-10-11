package com.zjl.spark.core.rdd.operator.transfer

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark12_RDD_Test {
  def main(args: Array[String]): Unit = {
    //TODO Spark - RDD -算子（方法）

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File_RDD")
    //创建Spark上下文环境对象（连接对象）
    val sc: SparkContext = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)
    //TODO    从服务器日志数据apache.log中获取用户请求URL资源路径
    val fileRDD = sc.textFile("input/apache.log")

    val urlRDD: RDD[String] = fileRDD.map(
      line => {
        val datas: Array[String] = line.split(" ")
        datas(6)
      }
    )

    urlRDD.collect().foreach(println)


    sc.stop()


  }
}
