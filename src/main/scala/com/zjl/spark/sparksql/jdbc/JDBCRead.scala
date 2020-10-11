package com.zjl.spark.sparksql.jdbc

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SparkSession}

object JDBCRead {
  def main(args: Array[String]): Unit = {
    //1.先创建及SparkSession
    val spark: SparkSession = SparkSession
      .builder()
      .appName("JDBCRead")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val url="jdbc:mysql://hadoop102:3306/test"
    val user="root"
    val password="000000"
    val dbtable="user"

    /*val df: DataFrame = spark.read
      .option("url",url)
      .option("user",user)
      .option("password",password)
      .option("dbtable",dbtable)
      .format("jdbc").load()*/
    val props = new Properties()
    props.put("user",user)
    props.put("password",password)
    val df: DataFrame = spark.read.jdbc(url, "user", props)

    df.show()

    spark.close()

  }

}

/**
 *
从jdbc读数据
 通用

 专用

 */