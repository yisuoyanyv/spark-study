package com.zjl.spark.core.rdd.operator.transfer

import org.apache.spark.{SparkConf, SparkContext}

object Spark21_RDD_Operator8 {
  def main(args: Array[String]): Unit = {
    //TODO Spark - RDD -算子（方法）

    //glom =>将每个分区的数据转换为数组
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File_RDD")
    //创建Spark上下文环境对象（连接对象）
    val sc: SparkContext = new SparkContext(sparkConf)

    val dataRDD = sc.makeRDD(List(1, 2, 3, 4, 5, 6), 3)

    //TODO 过滤
    //根据指定的规则对数据进行筛选过滤，满足条件的数据保留，不满足的数据丢弃
    val rdd = dataRDD.filter(
      num => {
        num % 2 == 0
      }
    )


    rdd.collect().foreach(println)

    sc.stop()


  }
}
