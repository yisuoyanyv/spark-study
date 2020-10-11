package com.zjl.spark.core.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object acc {
  def main(args: Array[String]): Unit = {
    //TODO Spark - 累加器

    //创建Spark运行配置对象
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File_RDD")

    //创建Spark上下文环境对象（连接对象）
    val sc: SparkContext = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4,5,6))

   val acc=sc.longAccumulator


    val resultRDD: RDD[Int] = rdd.map {
      x => {
        acc.add(1)
        x
      }
    }

    resultRDD.collect
    println(acc.value)

    sc.stop()


  }
}
