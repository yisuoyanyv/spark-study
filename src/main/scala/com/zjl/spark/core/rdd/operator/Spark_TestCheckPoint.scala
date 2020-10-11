package com.zjl.spark.core.rdd.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
 * RDD的 checkPoint
 *
 * 检查点：
 * 他的功能和持久化一致
   * 表现是不一样的
   * 1.checkpoint,需要手动指定存储目录
   * 2.checkpoint的时候，当第一个job执行完之后，spark内部会立即再起一个job，专门的去做checkpoint
 *    持久化会使用第一个job的结果进行持久化
 *    checkpoint会多起一个job
 *  3. checkpoint会切断他的血缘关系
 *      持久化不会切断血缘关系
 *
 */
object Spark_TestCheckPoint {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File_RDD")
    //创建Spark上下文环境对象（连接对象）
    val sc: SparkContext = new SparkContext(sparkConf)
    sc.setCheckpointDir("./ck1")

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

    /**
     * checkpoint和cache混合使用，可减少checkpoint的一次计算
     */
    mapRDD.checkpoint()
    mapRDD.cache()


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
