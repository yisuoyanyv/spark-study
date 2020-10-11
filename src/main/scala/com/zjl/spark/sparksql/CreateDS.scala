package com.zjl.spark.sparksql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object CreateDS {

  case class User(name:String,age:Int)
  def main(args: Array[String]): Unit = {

    //1.先创建及SparkSession
    val spark: SparkSession = SparkSession
      .builder().appName("CreateDF").master("local[*]")
      .getOrCreate()

    import spark.implicits._

//    val list1 = List(30, 40, 50, 70, 10, 20)
//
//    //把集合转成ds
//    val ds: Dataset[Int] = list1.toDS()
////    val df: DataFrame = list1.toDF()
//
//    //df能用的 ds一定可以用
//    ds.show()

    val list = List(User("zs", 10), User("lisi", 20), User("wg", 30))
    val ds: Dataset[User] = list.toDS()

    //在ds做sql查询
    ds.createOrReplaceTempView("user")
    spark.sql("select * from user where age >15").show


      //4.关闭SparkSession
    spark.stop()
  }

}
