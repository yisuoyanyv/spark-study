package com.zjl.spark.sparksql

import org.apache.spark.sql.{DataFrame, SparkSession}

object CreateDF {

  def main(args: Array[String]): Unit = {

    //1.先创建及SparkSession
    val spark: SparkSession = SparkSession
      .builder().appName("CreateDF").master("local[*]")
      .getOrCreate()

    //2.通过SparkSession创建DF
    val df: DataFrame = spark.read.json("e:/users.json")


    //3.对DF做操作(sql)
    //3.1创建临时表
    df.createOrReplaceTempView("user")
    //3.2查询临时表
    spark.sql("select * from user").show

    //4.关闭SparkSession
    spark.stop()
  }

}
