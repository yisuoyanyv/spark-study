package com.zjl.spark.core.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 查看血缘关系和依赖关系
 */
object Spark_TestJoin {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File_RDD")
    //创建Spark上下文环境对象（连接对象）
    val sc: SparkContext = new SparkContext(sparkConf)

    //3.1 创建第一个RDD
    val rdd: RDD[(Int,String)] = sc.makeRDD(Array((1,"a"),(2,"b"),(3,"c")))

    //3.2 创建第二个RDD
    val rdd1: RDD[(Int,Int)] = sc.makeRDD(Array((1,2),(2,5),(4,6)))

    //join算子相当于内连接，将两个RDD中的key相同的数据匹配，如果key匹配不上，那么数据不关联
//    val newRDD: RDD[(Int, (String, Int))] = rdd.join(rdd1)

//    val newRDD: RDD[(Int, (String, Option[Int]))] = rdd.leftOuterJoin(rdd1)


    //cogroup
    val newRDD: RDD[(Int, (Iterable[String], Iterable[Int]))] = rdd.cogroup(rdd1)
    newRDD.collect().foreach(println)



    //关闭连接
    sc.stop()
  }

}
