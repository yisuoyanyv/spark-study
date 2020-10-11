package com.zjl.spark.practice

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * 统计每一个省份广告点击排名前3的
 */
object agent {
  def main(args: Array[String]): Unit = {
    //创建Spark运行配置对象
    val sparkConf=new SparkConf().setMaster("local[*]").setAppName("WordCount")

    //创建Spark上下文环境对象（连接对象）
    val sc:SparkContext=new SparkContext(sparkConf)

    //1.读取文件数据  时间戳  省份id  城市id  用户id  广告id
    val logRDD:RDD[String]=sc.textFile("input/agent.log")
    //2.对读取到的数据，进行结构转换(省份id-广告id,1)
    val mapRDD: RDD[(String, Int)] = logRDD.map {
      line => {
        //2.1 用空格对读取的一行字符串进行切分
        val fields: Array[String] = line.split(" ")
        //2.2 封装为元组结构返回 (省份id-广告id,1)
        (fields(1) + "-" + fields(4), 1)
      }
    }

    //3.对当前省份的每一个广告点击次数进行聚合 (省份id-广告id,100)
    val reduceRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_ + _)

    //4. 再次对结构进行转换，将省份作为key  (省份A，(广告，点击次数））
    val map1RDD: RDD[(String, (String, Int))] = reduceRDD.map {
      case (proAndAd, clickCount) => {
        val proAndAdArr: Array[String] = proAndAd.split("-")
        (proAndAdArr(0), (proAndAdArr(1), clickCount))
      }
    }

    //5. 按照省份对数据进行分组   (省份A，Iterable[(广告，点击次数）,(广告，点击次数）]
    val groupRDD: RDD[(String, Iterable[(String, Int)])] = map1RDD.groupByKey()

    //6. 对每一个省份中的广告点击次数进行降序排序并取前三名
    val resRDD: RDD[(String, List[(String, Int)])] = groupRDD.mapValues(
      itr => {
        itr.toList.sortWith {
          (left, right) => {
            left._2 > right._2
          }
        }.take(3)
      }
    )


    resRDD.collect().foreach(println)
    //关闭连接
    sc.stop()
  }

}
