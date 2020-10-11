package com.zjl.spark.core.rdd.operator.transfer

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark09_RDD_Operator {
  def main(args: Array[String]): Unit = {
    //TODO Spark - RDD -算子（方法）
    //转换算子
    //能够将旧的RDD通过方法转换为新的RDD，但不会执行

    //创建Spark运行配置对象
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File_RDD")

    //创建Spark上下文环境对象（连接对象）
    val sc: SparkContext = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

    //旧RDD=》算子=》新RDD
    //    val rdd1 = rdd.map((i:Int) => {i*2} )
    //    val rdd1 = rdd.map((i:Int) => i*2 )
    //    val rdd1 = rdd.map((i) => i*2 )
    //    val rdd1 = rdd.map(i => i*2 )

    //2个分区=>12,34
    val rdd1: RDD[Int] = rdd.map(_ * 2)

    //TODO 分区问题
    //RDD中有分区列表
    //默认分区数量不变，数据会转换后输出

    val ints = rdd1.collect()
    println(ints.mkString(","))


    sc.stop()


  }
}
