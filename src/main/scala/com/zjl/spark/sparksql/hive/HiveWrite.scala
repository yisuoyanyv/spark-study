package com.zjl.spark.sparksql.hive

import org.apache.spark.sql.{DataFrame, SparkSession}

object HiveWrite {
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

//    val df: DataFrame = spark.read.json("file:///e:/users.json")  //这句执行 提示lzo类找不到。。。
    val df: DataFrame = (1 to 10).toDF("value")
    spark.sql("use spark1015")
    df.show()
    //直接把数据写入到hive中，表可以存在也可以不存在
//    df.write.mode("append").saveAsTable("user2")
    df.write.insertInto("user2") //基本等价于.mode("append").saveAsTable("user2")
    spark.close()

  }

}

/**
 *
从jdbc读数据
 通用

 专用

 */