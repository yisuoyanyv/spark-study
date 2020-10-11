package com.zjl.spark.core.rdd.basic

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_File {
  def main(args: Array[String]): Unit = {
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
    //val fileRDD:RDD[String] = sc.textFile(path = "input/1.txt")
    val fileRDD: RDD[String] = sc.textFile(path = "{input/*}") //读取目录的写法
    //文件路径可以采用通配符
    //文件路径还可以指向第三方存储系统：HDFS
    println(fileRDD.collect().mkString(","))


  }
}
