package com.zjl.spark.sparkstreaming.window

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Window2 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("wordcount1")
    val ssc = new StreamingContext(conf, Seconds(3))
    ssc.checkpoint("ck1")

    ssc
      .socketTextStream("hadoop102", 9999)
      .window(Seconds(9),Seconds(6))
      .flatMap(_.split("\\W+"))
      .map((_,1))
      .reduceByKey(_+_)
      .print()



    ssc.start()
    ssc.awaitTermination()
  }
}
