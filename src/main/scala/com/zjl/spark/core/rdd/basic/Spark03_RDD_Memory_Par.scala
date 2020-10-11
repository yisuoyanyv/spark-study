package com.zjl.spark.core.rdd.basic

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark03_RDD_Memory_Par {
  def main(args: Array[String]): Unit = {
    //创建Spark运行配置对象
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

    //创建Spark上下文环境对象（连接对象）
    val sc: SparkContext = new SparkContext(sparkConf)

    //TODO Spark-从内存中创建RDD

    //1.makeRDD的第一个参数：数据源
    //2.makeRDD的第二个参数：默认并行度（分区的数量）
    //一个cpu核对应一个taks  并行操作
    //RDD中的分区数量就是并行度，设定并行度，其实就是在设定分区数量
    //scheduler.conf.getInt("spark.default.parallelism", totalCores)
    //如果获取不到指定参数，会采用默认值(机器的总核数）
    //机器总核数=当前环境中可用核数  setMaster("local[*]")
    //local=>单核（单线程）=》1
    //local[4]=>4核=》4
    //local[8]=>最大核数=>8

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 3)
    //    rdd.collect().foreach(println)

    //将RDD的处理完后的数据保存到文件中
    rdd.saveAsTextFile("output")
  }
}
