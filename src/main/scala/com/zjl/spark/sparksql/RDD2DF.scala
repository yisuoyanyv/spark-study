package com.zjl.spark.sparksql

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object RDD2DF {
  case class User(name: String, age: Int)
  def main(args: Array[String]): Unit = {

    //1.先创建及SparkSession
    val spark: SparkSession = SparkSession
      .builder().appName("RDD2DF").master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val sc=spark.sparkContext

    val rdd=sc.parallelize(Array(User("lisi",10),User("zs",20),User("wf",30)))

    rdd.toDF("n","a").show

    val rddDS: Dataset[User] = rdd.toDS()
    rddDS.show()
    //4.关闭SparkSession
    spark.stop()
  }

}

class User(name:String,age: Int){

}
