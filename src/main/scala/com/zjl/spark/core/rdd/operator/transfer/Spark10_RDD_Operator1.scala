package com.zjl.spark.core.rdd.operator.transfer

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark10_RDD_Operator1 {
  def main(args: Array[String]): Unit = {
    //TODO Spark - RDD -算子（方法）
    //转换算子
    //能够将旧的RDD通过方法转换为新的RDD，但不会执行

    //创建Spark运行配置对象
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File_RDD")

    //创建Spark上下文环境对象（连接对象）
    val sc: SparkContext = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)

    //旧RDD=》算子=》新RDD
    //    val rdd1 = rdd.map((i:Int) => {i*2} )
    //    val rdd1 = rdd.map((i:Int) => i*2 )
    //    val rdd1 = rdd.map((i) => i*2 )
    //    val rdd1 = rdd.map(i => i*2 )
    val rdd1: RDD[Int] = rdd.map(_ * 2)

    //读取数据
    //collect方法不会转换RDD，会触发作业的执行
    //所以将collect这样的方法称之为行动(action)算子　　

    rdd1.saveAsTextFile("output")


    sc.stop()


  }
}
