package com.zjl.spark.sparksql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object DF2RDD {

  case class User(name:String,age:Int)
  def main(args: Array[String]): Unit = {

    //1.先创建及SparkSession
    val spark: SparkSession = SparkSession
      .builder()
      .appName("DF2RDD")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
   //直接从一个Scala的集合到df，做练习或测试用

//    val df:DataFrame=( 1 to 10 ).toDF("number")
//    //转rdd rdd中存储的一定是Row
//    val rdd: RDD[Row] = df.rdd
//    rdd.map(row=>row.getInt(0)).collect().foreach(println)

    val df: DataFrame = spark.read.json("e:/users.json")
    val rdd1: RDD[Row] = df.rdd
    rdd1.collect().foreach(println)

    val rddUser: RDD[User] = df.rdd.map(row => {
      User(row.getString(1), row.getLong(0).toInt)
    })
    rddUser.collect().foreach(println)


    //4.关闭SparkSession
    spark.stop()
  }

}




