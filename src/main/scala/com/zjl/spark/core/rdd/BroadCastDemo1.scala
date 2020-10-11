package com.zjl.spark.core.rdd

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 广播变量
 */
object BroadCastDemo1 {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File_RDD")
    //创建Spark上下文环境对象（连接对象）
    val sc: SparkContext = new SparkContext(sparkConf)

    val bigArr:Array[Int]=1 to 1000 toArray

    //广播变量，广播出去。
    val bd: Broadcast[Array[Int]] = sc.broadcast(bigArr)

    val list1=List(20,50000,70,6000,10)
    //3.1 创建第一个RDD
    val rdd1: RDD[(Int)] = sc.makeRDD(list1,4)

    //3.2 创建第二个RDD
    val rdd2: RDD[(Int)] = rdd1.filter(x => bd.value.contains(x))

    rdd2.collect().foreach(println)

    //关闭连接
    sc.stop()
  }

}
