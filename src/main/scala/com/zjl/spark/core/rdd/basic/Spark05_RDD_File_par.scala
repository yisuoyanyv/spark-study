package com.zjl.spark.core.rdd.basic

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark05_RDD_File_par {
  def main(args: Array[String]): Unit = {
    //TODO Scala
    //1. math.min
    //2. math.max
    //创建Spark运行配置对象
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File_RDD")

    //创建Spark上下文环境对象（连接对象）
    val sc: SparkContext = new SparkContext(sparkConf)

    //TODO Spark-从磁盘（File)中创建RDD
    //path:读取文件（目录）的路径
    //path可以设定相对路径，如果时IDEA，那么相对路径的位置从项目的根开始查找
    //path路径根据环境的不同自动发送改变

    //Spark读取文件时，默认采用的是Hadoop读取文件的规则
    //默认是一行一行的读取文件内容

    //textFile 第一个参数表示表示读取文件的路径
    //textFile 第二个参数表示最小分区数量
    //   math.min(defaultParallelism, 2)
    //   math.min(12, 2)=>2
    //    val fileRDD:RDD[String] = sc.textFile(path = "{input/*}")//读取目录的写法

    val fileRDD1: RDD[String] = sc.textFile(path = "input/3.txt")
    //    fileRDD1.saveAsTextFile("output1")

    val fileRDD2: RDD[String] = sc.textFile(path = "input/3.txt", 1)
    //    fileRDD2.saveAsTextFile("output2")

    val fileRDD3: RDD[String] = sc.textFile(path = "input/3.txt", 4)
    fileRDD3.saveAsTextFile("output3")

    val fileRDD4: RDD[String] = sc.textFile(path = "input/3.txt", 3)
    fileRDD3.saveAsTextFile("output4")
    sc.stop()


  }
}
