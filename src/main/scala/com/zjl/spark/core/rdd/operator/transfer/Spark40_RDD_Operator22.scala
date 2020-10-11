package com.zjl.spark.core.rdd.operator.transfer

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark40_RDD_Operator22 {
  def main(args: Array[String]): Unit = {
    //TODO Spark - RDD -算子（方法）

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File_RDD")
    //创建Spark上下文环境对象（连接对象）
    val sc: SparkContext = new SparkContext(sparkConf)

    val rdd: RDD[(String, Int)] = sc.makeRDD(
      List(
        ("a", 1), ("a", 2), ("c", 3),
        ("b", 4), ("c", 2), ("c", 6)
      ),2
    )

    //如果分区内计算规则和分区间计算规则相同都是求和，那么可以计算wordcount
//    val result: RDD[(String, Int)] = rdd.aggregateByKey(0)(
//      (x, y) => x + y,
//      (x, y) => x + y
//    )
//    val result: RDD[(String, Int)] = rdd.aggregateByKey(0)(_ + _, _ + _)


    //如果分区内计算规则和分区间计算规则相同，那么可以将aggregateByKey简化为
    //另外一个方法  foldByKey
    val result: RDD[(String, Int)] = rdd.foldByKey(0)(_ + _)
    println(result.collect().mkString(","))


    sc.stop()

  }

  
}
