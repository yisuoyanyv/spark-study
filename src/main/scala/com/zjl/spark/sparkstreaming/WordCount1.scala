package com.zjl.spark.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}


object WordCount1 {
  def main(args: Array[String]): Unit = {

    //1.创建StreamingContext
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("wordcount1")
    val ssc = new StreamingContext(conf, Seconds(3))
    //2.从数据源创建一个流：  socket,rdd队列，自定义接收器， kafka(重点)
    val sourceStream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102", 9999)
    //3.对流做各种转换
    val result: DStream[(String, Int)] = sourceStream.flatMap(_.split(" ")).map((_, 1)).reduceByKey((_ + _))
    //4.行动算子 print  foreach  foreachRDD
    result.print() //把结果打印在控制台
    //5.启动流
    ssc.start()
    //6.阻止主线程退出(阻塞主线程)
    ssc.awaitTermination()

    println("异常退出")
  }

}
