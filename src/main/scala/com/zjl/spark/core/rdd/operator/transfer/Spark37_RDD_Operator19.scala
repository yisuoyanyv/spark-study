package com.zjl.spark.core.rdd.operator.transfer

import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, SparkContext}

object Spark37_RDD_Operator19 {
  def main(args: Array[String]): Unit = {
    //TODO Spark - RDD -算子（方法）

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File_RDD")
    //创建Spark上下文环境对象（连接对象）
    val sc: SparkContext = new SparkContext(sparkConf)

    //TODO reduceByKey:根据数据的key进行分区，然后对value进行聚合。分组+聚合

    val rdd=sc.makeRDD(
      List(
        ("hello",1),("scale",1),("hello",1)
      )
    )
    //word=>(word,1)
    //reduceByKey 第一个参数表示相同的key的value的聚合方式
    //reduceByKey 第二个参数表示聚合后的分区数量
    val rdd1 = rdd.reduceByKey(_ + _)
    val rdd2 = rdd.reduceByKey(_ + _,2)
    println(rdd1.collect().mkString(","))

    sc.stop()

  }

  //TODO 自定义分区器
  //1.和Partitioner类发生关联，继承Partitioner
  //2.重写方法
  class MyPartitioner(num:Int) extends Partitioner{
    //获取分区的数量
    override def numPartitions: Int = {
      num
    }
    //根据数据的key来决定数据在哪个分区中进行处理
    //方法的返回值表示分区编号（索引）
    override def getPartition(key: Any): Int = {
      key match{
        case "cba"=>0
        case _=>1
      }
    }
  }
}
