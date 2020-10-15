package com.zjl.spark.sparkstreaming.transform

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object TransformDemo1 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("wordcount1")
    val ssc = new StreamingContext(conf, Seconds(3))

    val sourceStream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102", 9999)

    val result: DStream[(String, Int)] = sourceStream.transform(rdd => {
      rdd.flatMap(_.split("\\W+")).map((_, 1)).reduceByKey(_ + _)
    })
    result.print
    result.foreachRDD(rdd=>{

    })
    ssc.start()

    ssc.awaitTermination()
  }
}
