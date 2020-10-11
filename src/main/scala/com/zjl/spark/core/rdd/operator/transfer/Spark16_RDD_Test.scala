package com.zjl.spark.core.rdd.operator.transfer

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark16_RDD_Test {
  def main(args: Array[String]): Unit = {
    //TODO Spark - RDD -算子（方法）


    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File_RDD")
    //创建Spark上下文环境对象（连接对象）
    val sc: SparkContext = new SparkContext(sparkConf)

    //获取第二个数据分区的数据
    val dataRDD: RDD[Int] = sc.makeRDD(List(1, 4, 3, 2, 5, 6), 3)

    //获取的分区索引从0开始
    val rdd = dataRDD.mapPartitionsWithIndex(
      (index, iter) => {
        if (index == 1) {
          iter
        } else {
          Nil.iterator
        }

      }
    )
    println(rdd.collect().mkString(","))

    sc.stop()


  }
}
