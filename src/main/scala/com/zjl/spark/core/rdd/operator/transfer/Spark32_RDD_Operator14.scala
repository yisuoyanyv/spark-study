package com.zjl.spark.core.rdd.operator.transfer

import org.apache.spark.{SparkConf, SparkContext}

object Spark32_RDD_Operator14 {
  def main(args: Array[String]): Unit = {
    //TODO Spark - RDD -算子（方法）

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File_RDD")
    //创建Spark上下文环境对象（连接对象）
    val sc: SparkContext = new SparkContext(sparkConf)

    //TODO sortBy
    //默认排序规则为升序
    //sortBy可以通过传递第二个参数改变排序的方式
    //sortBy可以设定第三个参数改变分区
    val dataRDD = sc.makeRDD(List(1,4,2,3), 3)
    val sortRDD=dataRDD.sortBy(num=>num,false)
    println(sortRDD.collect().mkString(","))

    sc.stop()


  }
}
