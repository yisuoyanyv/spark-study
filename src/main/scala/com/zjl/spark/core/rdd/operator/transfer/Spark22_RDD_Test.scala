package com.zjl.spark.core.rdd.operator.transfer

import org.apache.spark.{SparkConf, SparkContext}

object Spark22_RDD_Test {
  def main(args: Array[String]): Unit = {
    //将List(List(1,2),3,List(4,5))进行扁平化操作


    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File_RDD")
    //创建Spark上下文环境对象（连接对象）
    val sc: SparkContext = new SparkContext(sparkConf)



    //glom
    //计算所有分区最大值求和（分区内取最大值，分区间最大值求和）

    val dataRDD = sc.makeRDD(List(1, 2, 3, 4, 5, 6), 2)

    //将每个分区的数据转换为数组
    val glomRDD = dataRDD.glom()

    //将数组中的最大值取出
    //Array=>max
    val maxRDD = glomRDD.map(array => array.max)

    //将取出的最大值求和
    val arr = maxRDD.collect()
    println(arr.sum)
    sc.stop()


  }
}
