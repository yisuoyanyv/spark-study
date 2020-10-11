package com.zjl.spark.sparksql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object DS2RDD {

  case class User(name:String,age:Int)
  def main(args: Array[String]): Unit = {

    //1.先创建及SparkSession
    val spark: SparkSession = SparkSession
      .builder()
      .appName("DF2RDD")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val ds: Dataset[User] = Seq(User("lisi", 40), User("zs", 30), User("dd", 40)).toDS()

    val rdd: RDD[User] = ds.rdd
    rdd.collect().foreach(println)



    //4.关闭SparkSession
    spark.stop()
  }

}




