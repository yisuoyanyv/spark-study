package com.zjl.spark.sparksql.hive

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SparkSession}

object HiveRead {
  def main(args: Array[String]): Unit = {
    //1.先创建及SparkSession
    val spark: SparkSession = SparkSession
      .builder()
      .appName("HiveRead")
      .master("local[*]")
      //添加支持外置hive
      .enableHiveSupport()
      .getOrCreate()
    import  spark.implicits._

    spark.sql("show databases").show()
    spark.sql("use gmall").show()
    spark.sql("show tables").show()

    spark.close()

  }

}

/**
 *
从jdbc读数据
 通用

 专用

 */