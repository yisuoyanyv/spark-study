package com.zjl.spark.sparksql.jdbc

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object JDBCWrite {
  val url="jdbc:mysql://hadoop102:3306/test"
  val user="root"
  val password="000000"
  val dbtable="user"
  def main(args: Array[String]): Unit = {
    //1.先创建及SparkSession
    val spark: SparkSession = SparkSession
      .builder()
      .appName("JDBCRead")
      .master("local[*]")
      .getOrCreate()
    import spark.implicits._

    val df: DataFrame = spark.read.json("e:/users.json")

/*
    df.write.format("jdbc")
      .option("url",url)
      .option("user",user)
      .option("password",password)
      .option("dbtable","userzjl")
//      .mode("append")
      .mode(SaveMode.Append)
      .save()

*/

    val props = new Properties()
    props.put("user",user)
    props.put("password",password)
    df.write.jdbc(url,"user1016",props)




    spark.close()

  }

}

/**
 *
从jdbc读数据
 通用

 专用

 */