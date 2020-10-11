package com.zjl.spark.sparksql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object DFDS {

  case class People(user:String,age:Long)
  def main(args: Array[String]): Unit = {

    //1.先创建及SparkSession
    val spark: SparkSession = SparkSession
      .builder()
      .appName("DF2RDD")
      .master("local[*]")
      .getOrCreate()
    import spark.implicits._
   //直接从一个Scala的集合到df，做练习或测试用
    val df: DataFrame = spark.read.json("e:/users.json")
    //现有一个样例类
    val ds: Dataset[People] = df.as[People]

    val df1: DataFrame = ds.toDF()
    df1.show()
    //4.关闭SparkSession
    spark.stop()
  }

}




