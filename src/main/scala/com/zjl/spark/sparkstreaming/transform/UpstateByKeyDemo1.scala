package com.zjl.spark.sparkstreaming.transform

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object UpstateByKeyDemo1 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("wordcount1")
    val ssc = new StreamingContext(conf, Seconds(3))
    ssc.checkpoint("ck1")

    ssc
      .socketTextStream("hadoop102", 9999)
      .flatMap(_.split("\\W+"))
      .map((_,1))
      .updateStateByKey((seq:Seq[Int],opt:Option[Int]) => {
        Some(seq.sum+opt.getOrElse(0))
      })
      .print(1000)

    ssc.start()

    ssc.awaitTermination()
  }
}
