package com.zjl.spark.core.rdd.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 *aggregate  fold
 */
object Spark_action02 {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File_RDD")
    //创建Spark上下文环境对象（连接对象）
    val sc: SparkContext = new SparkContext(sparkConf)

//    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4),8)

//    val res: Int = rdd.aggregate(0)(_ + _, _ + _)
//
//    println(res)
//    val res1: Int = rdd.aggregate(10)(_ + _, _ + _)
//
//    println(res1)

//    val res: Int = rdd.fold(10)(_ + _)
//    println(res)


    //countByKey 统计每种key出现的次数
//    val rdd: RDD[(Int, String)] = sc.makeRDD(List((1, "a"), (1, "a"), (1, "a"), (2, "b"), (3, "c"), (3, "c")))
//    val res: collection.Map[Int, Long] = rdd.countByKey()
//    println(res)


    //save相关
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)
    //保存为文本文件
    rdd.saveAsTextFile("output")
    //保存为序列化文件
    rdd.saveAsObjectFile("output1")
    //保存为SequenceFile  注意：只支持kv类型RDD
    rdd.map((_,2)).saveAsSequenceFile("output2")
    //关闭连接
    sc.stop()
  }

}
