package com.zjl.spark.sparksql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object RDD2DF2 {

  def main(args: Array[String]): Unit = {

    //1.先创建及SparkSession
    val spark: SparkSession = SparkSession
      .builder().appName("RDD2DF").master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val sc=spark.sparkContext
    val rdd: RDD[Row] = sc.parallelize((("lisi", 10) :: ("zs", 20) :: Nil)).map {
      case (name, age) => Row(name, age)
    }

    val schema=StructType(Array(StructField("name",StringType),StructField("age",IntegerType)))
    //使用提供了一些api
    val df: DataFrame = spark.createDataFrame(rdd, schema = schema)
    df.show
    //4.关闭SparkSession
    spark.stop()
  }

}



