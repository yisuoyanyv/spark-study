package com.zjl.spark.core.rdd.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
 * RDD的缓存
 */
object Spark_TestCache {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File_RDD")
    //创建Spark上下文环境对象（连接对象）
    val sc: SparkContext = new SparkContext(sparkConf)

    //创建RDD
    val rdd: RDD[String] = sc.makeRDD(List("hello zjl","hello ctt"), 2)

    //扁平映射
    val flatMapRDD: RDD[String] = rdd.flatMap(_.split(" "))

    //结构转换
    val mapRDD: RDD[(String, Int)] = flatMapRDD.map {
      word => {
        println("*****************")
        (word, 1)
      }
    }
    //打印血缘关系
    println(mapRDD.toDebugString)

    //对RDD的数据进行缓存  底层调用的是persist函数  默认缓存在内存中
//    mapRDD.cache()

    //persist可以接收参数  指定缓存的位置
    //注意：虽然叫持久化，但是当应用程序执行结束之后，缓存的目录也会被删除
    mapRDD.persist(StorageLevel.DISK_ONLY)


    //触发行动操作
    mapRDD.collect()
    println("-----------------------")

    //打印血缘关系
    println(mapRDD.toDebugString)
    //触发行动操作
    mapRDD.collect()

    //关闭连接
    sc.stop()
  }

}
