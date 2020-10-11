package com.zjl.spark.core.rdd.operator.transfer

import org.apache.spark.{SparkConf, SparkContext}

object Spark23_RDD_Test {
  def main(args: Array[String]): Unit = {
    //将List(List(1,2),3,List(4,5))进行扁平化操作


    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File_RDD")
    //创建Spark上下文环境对象（连接对象）
    val sc: SparkContext = new SparkContext(sparkConf)

    //根据单词的首写字母进行分组
    val dataRDD=sc.makeRDD(List("Hello","Hive","hbase","Hadoop"),2)

    dataRDD.groupBy(word=>{
//      word.substring(0,1)
//      word(0)
      word.charAt(0)
    })
    sc.stop()


  }
}
