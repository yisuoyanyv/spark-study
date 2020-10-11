package com.zjl.spark.core.rdd.operator

import org.apache.spark.rdd.{JdbcRDD, RDD}
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

object PartitionerDemo1 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File_RDD")
    //创建Spark上下文环境对象（连接对象）
    val sc: SparkContext = new SparkContext(sparkConf)

    val rdd: RDD[(Char, Int)] = sc.makeRDD(List(('a', 1), ('b', 2), ('c', 2), ('d', 4),('f',0)),4)
    val resultRDD: RDD[(Char, Int)] = rdd.partitionBy(new MyPartitioner(2))
    resultRDD.glom().map(_.toList).collect().foreach(println)


    sc.stop()

  }

}

//自定义的Hash分区器
class MyPartitioner(num:Int) extends Partitioner{

  assert(num>0)

  override def numPartitions: Int = num

  override def getPartition(key: Any): Int = key match {
    case null => 0
    case _ => key.hashCode().abs % num
  }
}
