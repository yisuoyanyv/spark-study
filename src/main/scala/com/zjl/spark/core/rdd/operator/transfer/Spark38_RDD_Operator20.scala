package com.zjl.spark.core.rdd.operator.transfer

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

object Spark38_RDD_Operator20 {
  def main(args: Array[String]): Unit = {
    //TODO Spark - RDD -算子（方法）

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File_RDD")
    //创建Spark上下文环境对象（连接对象）
    val sc: SparkContext = new SparkContext(sparkConf)

    //TODO reduceByKey:根据数据的key进行分区，然后对value进行聚合。分组+聚合

    val rdd: RDD[(String, Int)] = sc.makeRDD(
      List(
        ("hello", 1), ("scale", 1), ("hello", 1)
      )
    )
    //TODO groupByKe:根据数据的key进行分组
    //groupBy:根据指定的规则对数据进行分组
    //调用groupByKey后，返回数据的类型为元组
    //  元组的第一个元素表示的是用于分组的key
    //  元组的第二个元素表示的是分组后，相同key的value的集合
    val groupRDD: RDD[(String, Iterable[Int])] = rdd.groupByKey()
    val wordToCount: RDD[(String, Int)] = groupRDD.map {
      case (word, iter) => {
        (word, iter.size)
      }
    }
    println(wordToCount.collect().mkString(","))

    sc.stop()

  }


}
