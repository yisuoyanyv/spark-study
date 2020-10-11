package com.zjl.spark.core.rdd.basic

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Memory {
  def main(args: Array[String]): Unit = {
    //创建Spark运行配置对象
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

    //创建Spark上下文环境对象（连接对象）
    val sc: SparkContext = new SparkContext(sparkConf)

    //TODO Spark-从内存中创建RDD

    //1.parallelize: 并行
    val list = List(1, 2, 3, 4)
    val rdd: RDD[Int] = sc.parallelize(list)
    rdd.collect().foreach(println)

    val rdd1: RDD[Int] = sc.makeRDD(list)
    println(rdd1.collect().mkString(","))
    sc.stop()
  }
}
