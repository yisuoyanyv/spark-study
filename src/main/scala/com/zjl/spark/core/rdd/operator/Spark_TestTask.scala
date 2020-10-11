package com.zjl.spark.core.rdd.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Task 和job
 */
object Spark_TestTask {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File_RDD")
    //创建Spark上下文环境对象（连接对象）
    val sc: SparkContext = new SparkContext(sparkConf)

    //创建RDD
    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,8,4,5), 2)

    val resultRDD: RDD[(Int, Int)] = rdd.map((_, 1)).reduceByKey(_ + _)

    //Job：一个Action算子就会生成一个Job：
    //job1打印到控制台
    resultRDD.collect().foreach(println)
    //job2输出到磁盘
    resultRDD.saveAsTextFile("output")

    Thread.sleep(10000000)

    //关闭连接
    sc.stop()
  }

}
