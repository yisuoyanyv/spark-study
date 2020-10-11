package com.zjl.spark.core.rdd.operator.transfer

import org.apache.spark.{SparkConf, SparkContext}

object Spark33_RDD_Operator15 {
  def main(args: Array[String]): Unit = {
    //TODO Spark - RDD -算子（方法）

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File_RDD")
    //创建Spark上下文环境对象（连接对象）
    val sc: SparkContext = new SparkContext(sparkConf)


    val rdd1 = sc.makeRDD(List(1,4,2,3),2)
    val rdd2 = sc.makeRDD(List(3,4,5,6),2)

    //TODO 并集 ，数据合并，分区也会合并

    val rdd3 = rdd1.union(rdd2)
//    println(rdd3.collect().mkString(","))
    rdd3.saveAsTextFile("output3")
   //TODO 交集：保留最大分区数  分区数不变，数据会被打乱，有shuffle
    val rdd4 = rdd1.intersection(rdd2)
//    println(rdd4.collect().mkString(","))
    rdd4.saveAsTextFile("output4")


    // TODO 差集： 数据被打乱重组，shuffle
    //当调用rdd的subtract方法时，以当前的rdd的分区为主，所以分区数量等于当前rdd的分区数
    val rdd5 = rdd1.subtract(rdd2)
//    println(rdd5.collect().mkString(","))
    rdd5.saveAsTextFile("output5")


    //TODO 拉链：分区数不变
    //2个RDD分区一致,但是数据量相同的场合：
    //  Exception: Can only zip RDDs with same number of elements in each partition
    //TODO 2个RDD的分区不一致，数据量也不相同，但是每个分区数据量一致：
    //  Exception:Can't zip RDDs with unequal numbers of partitions:List(3,2)
    val rdd6 = rdd1.zip(rdd2)
//    println(rdd6.collect().mkString(","))

    rdd6.saveAsTextFile("output6")

    sc.stop()


  }
}
