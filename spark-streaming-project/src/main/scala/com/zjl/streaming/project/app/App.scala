package com.zjl.streaming.project.app

import com.zjl.streaming.project.bean.AdsInfo
import com.zjl.streaming.project.util.MyKafkaUtils
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}

trait App {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("App")
    val ssc = new StreamingContext(conf, Seconds(3))
    ssc.checkpoint("./ck1020")



    val sourceStream =MyKafkaUtils.getKafkaStream(ssc,"ads_log")

    val adsInfoStream: DStream[AdsInfo] = sourceStream.map(s => {
      val split: Array[String] = s.split(",")
      AdsInfo(split(0).toLong, split(1), split(2), split(3),split(4))
    })

    doSomething(adsInfoStream)

    ssc.start()
    ssc.awaitTermination()
  }

  def doSomething(adsInfoStream:DStream[AdsInfo]):Unit
}
