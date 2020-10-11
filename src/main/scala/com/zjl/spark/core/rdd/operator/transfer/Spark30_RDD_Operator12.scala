package com.zjl.spark.core.rdd.operator.transfer

import org.apache.spark.{SparkConf, SparkContext}

object Spark30_RDD_Operator12 {
  def main(args: Array[String]): Unit = {
    //TODO Spark - RDD -算子（方法）

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File_RDD")
    //创建Spark上下文环境对象（连接对象）
    val sc: SparkContext = new SparkContext(sparkConf)

    val dataRDD = sc.makeRDD(List(1, 1, 1,4, 4,  4), 2)


    //TODO 扩大分区
    //coalesce主要目的是缩减分区，扩大分区没有效果
    //为什么不能扩大分区，因为在分区缩减时，数据不会打乱重新组合，没有shuffle的过程

    //如果就是非得想要将数据扩大分区，那么必须打乱数据进行重新组合，必须使用shuffle

    //TODO coalesce方法第一个参数表示缩减分区后的分区数量
    //TODO coalesce方法第二个参数表示分区改变时，是否会打乱重新组合数据，默认不打乱
    val coalesceRDD = dataRDD.coalesce(6,true)
    coalesceRDD.saveAsTextFile("output")
    sc.stop()


  }
}
