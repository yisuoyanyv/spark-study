package com.zjl.spark.sparkstreaming.window

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.ReceiverInputDStream

object Window1 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("wordcount1")
    val ssc = new StreamingContext(conf, Seconds(3))

    ssc
      .socketTextStream("hadoop102", 9999)
      .flatMap(_.split("\\W+"))
      .map((_,1))
      .reduceByKeyAndWindow(_+_,Seconds(6))
      .print()


    ssc.start()
    ssc.awaitTermination()
  }
}
