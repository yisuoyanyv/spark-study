package com.zjl.spark.sparksql.hive

import org.apache.spark.sql.{DataFrame, SparkSession}

object HiveWrite2 {
  def main(args: Array[String]): Unit = {

    System.setProperty("HADOOP_USER_NAME","zjl")
    //1.先创建及SparkSession
    val spark: SparkSession = SparkSession
      .builder()
      .appName("HiveWrite")
      .master("local[*]")
      //添加支持外置hive
      .enableHiveSupport()
      .config("spark.sql.warehouse.dir","hdfs://hadoop102:9000/user/hive/warehouse") //该句为了使创建的表，在hdfs路径上。
      .getOrCreate()

    import spark.implicits._

    //先创建一个数据库
//    spark.sql("create database spark1015").show
//    spark.sql("use spark1015").show
//    spark.sql("create table user1(id int,name string)").show
//    spark.sql("insert into  user1 values(10,'lisi')").show

    val df: DataFrame = spark.read.json("file:///e:/users.json")  //这句执行 提示lzo类找不到。。。,注释掉idea里的core-site中的相关lzo配置，可以正常执行
    df.createOrReplaceTempView("a")
    spark.sql("use spark1015")
    val df1: DataFrame = spark.sql("select * from a")
    val df2: DataFrame = spark.sql("select sum(age) sum_age from a group by user")
    println(df1.rdd.getNumPartitions)
    println(df2.rdd.getNumPartitions)  //分区数会成为200
    df1.write.saveAsTable("a1")
    df2.coalesce(1).write.mode("overwrite").saveAsTable("a2")
    spark.close()

  }

}

/**
 *
从jdbc读数据
 通用

 专用

 */