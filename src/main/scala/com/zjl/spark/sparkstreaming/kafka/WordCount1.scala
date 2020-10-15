package com.zjl.spark.sparkstreaming.kafka

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils


object WordCount1 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("wordcount1")

    val ssc = new StreamingContext(conf, Seconds(3))

    val params: Map[String, String] = Map[String, String](
      "bootstrap.servers" -> "hadoop102:9092,hadoop103:9092,hadoop104:9092",
      "group.id" -> "1015"
    )
     KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc,
      params,
      Set("first1015")
    )
    .flatMap{
      case (_,v)=>
        v.split("\\W+")
    }.map((_,1))
      .reduceByKey(_+_)
      .print()

//    sourceStream.print

    ssc.start()
    ssc.awaitTermination()
  }

}
