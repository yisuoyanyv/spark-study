package com.zjl.spark.core.rdd.basic

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark06_RDD_File_PartitionData {
  def main(args: Array[String]): Unit = {
    //TODO Scala
    //1. math.min
    //2. math.max
    //创建Spark运行配置对象
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File_RDD")

    //创建Spark上下文环境对象（连接对象）
    val sc: SparkContext = new SparkContext(sparkConf)

    //TODO Spark-从磁盘（File)中创建RDD
    //1.Spark读取文件采用的是Hadoop的读取规则
    //    文件切片规则  ：以字节方式来切片
    //    数据读取规则： 以行为单位来读取

    //2.问题
    //    TODO 文件到底切成几片（分区的数量)?
    //    文件字节数（10，包括回车换行），预计切片数量（2）
    //10/2=》5byte

    //totalSize=11
    //goalSize =totalSize/numSplits =11/2 =5 ... 1 =>3

    //所谓的最小分区数，取决于总的字节数是否能整除分区数，并且剩余的字节数达到一个比率
    //实际产生的分区数数量可能大于最小分区数

    //    TODO 分区的数据如何存储
    //   分区数据是以行为单位读取的，而不是字节


    val fileRDD1: RDD[String] = sc.textFile(path = "input/3.txt", 2)
    fileRDD1.saveAsTextFile("output")

    sc.stop()


  }
}
