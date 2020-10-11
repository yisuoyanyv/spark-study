package com.zjl.spark.core.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 查看血缘关系和依赖关系
 */
object Spark_TestLineage {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File_RDD")
    //创建Spark上下文环境对象（连接对象）
    val sc: SparkContext = new SparkContext(sparkConf)

    //创建RDD
    val rdd: RDD[String] = sc.makeRDD(List("hello spark", "hello world"), 2)

    //查看RDD的血缘关系
    println(rdd.toDebugString)
    //查看RDD的依赖关系
    println(rdd.dependencies)
    println("---------------------------------")

    val flatMapRDD: RDD[String] = rdd.flatMap(_.split(" "))
    //查看RDD的血缘关系
    println(flatMapRDD.toDebugString)
    //查看RDD的依赖关系
    println(flatMapRDD.dependencies)
    println("---------------------------------")

    val mapRDD: RDD[(String, Int)] = flatMapRDD.map((_, 1))
    println(mapRDD.toDebugString)
    println(mapRDD.dependencies)
    println("---------------------------------")

    val resRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_ + _)
    println(resRDD.toDebugString)
    println(resRDD.dependencies)
    println("---------------------------------")


    val resRDD1: RDD[(String, Int)] = mapRDD.reduceByKey(_ + _).map(t=>(t._1,t._2+1))
    println(resRDD1.toDebugString)
    println(resRDD1.dependencies)
    println("---------------------------------")


    //关闭连接
    sc.stop()
  }

}
