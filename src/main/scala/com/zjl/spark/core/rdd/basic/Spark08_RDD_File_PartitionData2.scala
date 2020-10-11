package com.zjl.spark.core.rdd.basic

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark08_RDD_File_PartitionData2 {
  def main(args: Array[String]): Unit = {
    //TODO Scala
    //1. math.min
    //2. math.max
    //创建Spark运行配置对象
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File_RDD")

    //创建Spark上下文环境对象（连接对象）
    val sc: SparkContext = new SparkContext(sparkConf)

    // (0, 0+2)
    // totalsize=6 num=2
    // 6/2=3
    //(0,0+3)=>(0,3)
    //(3,3+3)=>(3,6)

    //1@@=>012
    //234=>345

    //12/3=》4
    //1.txt => (0,0+4)
    //     =>(4,4+2)
    //2.txt=>(0,0+4)
    //     =>(4,4+2)


    //TODO hadoop 分区是以文件为单位进行划分的。
    //  读取数据不能跨越文件

    val fileRDD1: RDD[String] = sc.textFile(path = "{input/*}", 3)
    fileRDD1.saveAsTextFile("output")

    sc.stop()


  }
}
