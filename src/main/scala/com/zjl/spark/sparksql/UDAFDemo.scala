package com.zjl.spark.sparksql

import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object UDAFDemo {
  def main(args: Array[String]): Unit = {
    //在sql中，集合函数如何使用
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("UDAFDemo")
      .getOrCreate()

    import spark.implicits._
    val df: DataFrame = spark.read.json("e:/users.json")
    df.createOrReplaceTempView("user")
    //注册聚合函数
    spark.udf.register("mySum",new MySum)

    spark.sql("select mySum(age) from user").show

  }

}

class MySum extends UserDefinedAggregateFunction {
  //用来定义输入的数据类型  10.1 12.2 100
  override def inputSchema: StructType = StructType(StructField("ele",DoubleType)::Nil)
    //缓存区的类型
  override def bufferSchema: StructType = StructType(StructField("sum",DoubleType)::Nil)

  //最终聚合结果的类型
  override def dataType: DataType = DoubleType

  //相同的输入是否返回相同的输出
  override def deterministic: Boolean = true

  //对缓冲区初始化
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    //在缓存集合中初始化和
    buffer(0)=0D//等价于buffer.update(0,0D)

  }

  //分区内聚合
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    //input 是指使用聚合函数的时候，传过来的参数封装到了Row
    if(!input.isNullAt(0)){//考虑到传字段可能是null
      val v: Double = input.getAs[Double](0) //getDouble(0)
      buffer(0)=buffer.getDouble(0)+v
    }
  }

  //分区间聚合
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    //把buffer1和buffer2的缓冲聚合到一起，再把值写回到buffer1
    buffer1(0)=buffer1.getDouble(0)+buffer2.getDouble(0)
  }

  //返回最终的输出值
  override def evaluate(buffer: Row): Any = buffer.getDouble(0)
}
