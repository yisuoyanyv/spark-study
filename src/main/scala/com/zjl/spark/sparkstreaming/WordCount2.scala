package com.zjl.spark.sparkstreaming

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable


object WordCount2 {
  def main(args: Array[String]): Unit = {

    //从RDD队列中读取数据,仅仅用于做压力测试
    val conf: SparkConf = new SparkConf().setAppName("wordCount2").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(3))

    val rdds=mutable.Queue[RDD[Int]]()
    val sourceStream: InputDStream[Int] = ssc.queueStream(rdds,false)
    val result: DStream[Int] = sourceStream.reduce(_ + _)
    result.print()
    ssc.start()

    val sc: SparkContext = ssc.sparkContext
    while (true) {
      rdds.enqueue(sc.parallelize(1 to 100))
      Thread.sleep(2000)
    }

    ssc.awaitTermination()
  }

}
