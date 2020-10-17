package com.zjl.spark.sparkstreaming.out

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object OutDemo1 {

  val props=new Properties()
  props.setProperty("user","root")
  props.setProperty("password","000000")
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("wordcount1")
    val ssc = new StreamingContext(conf, Seconds(3))
    ssc.checkpoint("ck1")

    ssc
      .socketTextStream("hadoop102", 9999)
      .flatMap(_.split("\\W+"))
      .map((_,1))
//      .reduceByKey(_+_)
      .updateStateByKey((seq:Seq[Int],opt:Option[Int])=>Some(seq.sum+opt.getOrElse(0)))
//      .saveAsTextFiles("word","log")  //保存到了 hdfs下 hdfs://hadoop102:9000/user/zjl/word-1602942294000.log/_temporary/0/task_20201017214454_0045_m_000008
      .foreachRDD(rdd=>{
//        rdd.foreachPartition(it=>{
//          //连接到mysql
//
//          //写数据
//
//          //关闭mysql
//
//
//        })

        //把rdd转为df
        //1.先创建sparksession
        val spark: SparkSession = SparkSession.builder()
          .config(rdd.sparkContext.getConf)
          .getOrCreate()
        import spark.implicits._
        //2.转换
        val df: DataFrame = rdd.toDF("word", "count")

        //3.写
//        df.write.mode("append").jdbc("jdbc:mysql://hadoop102:3306/test","wordcount",props)
        df.write.mode("overwrite").jdbc("jdbc:mysql://hadoop102:3306/test","wordcount",props)
      })


    ssc.start()
    ssc.awaitTermination()
  }
}
