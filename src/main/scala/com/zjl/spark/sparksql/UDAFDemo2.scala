package com.zjl.spark.sparksql

import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object UDAFDemo2 {
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
    spark.udf.register("myAvg",new MyAvg)

    spark.sql("select myAvg(age) from user").show

  }

}

class MyAvg extends UserDefinedAggregateFunction {
  //用来定义输入的数据类型  10.1 12.2 100
  override def inputSchema: StructType = StructType(StructField("ele",DoubleType)::Nil)
    //缓存区的类型
  override def bufferSchema: StructType = StructType(StructField("sum",DoubleType)::
    StructField("count",LongType)::Nil)

  //最终聚合结果的类型
  override def dataType: DataType = DoubleType

  //相同的输入是否返回相同的输出
  override def deterministic: Boolean = true

  //对缓冲区初始化
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    //在缓存集合中初始化和
    buffer(0)=0D//等价于buffer.update(0,0D)
    buffer(1)=0L

  }

  //分区内聚合
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {

    input match {
      case Row(age:Double)=>
        buffer(0)=buffer.getDouble(0)+age
        buffer(1)=buffer.getLong(1)+1L
      case _ =>
    }

    //input 是指使用聚合函数的时候，传过来的参数封装到了Row
//    if(!input.isNullAt(0)){//考虑到传字段可能是null
//      val v: Double = input.getAs[Double](0) //getDouble(0)
//      buffer(0)=buffer.getDouble(0)+v
//      buffer(1)=buffer.getLong(1)+1L
//    }
  }

  //分区间聚合
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer2 match {
      case Row(sum:Double,count:Long)=>
        buffer1(0)=buffer1.getDouble(0)+sum
        buffer1(1)=buffer1.getLong(1)+count
      case _=>
    }

    //把buffer1和buffer2的缓冲聚合到一起，再把值写回到buffer1
//    buffer1(0)=buffer1.getDouble(0)+buffer2.getDouble(0)
//    buffer1(1)=buffer1.getLong(1)+buffer2.getLong(1)
  }

  //返回最终的输出值
  override def evaluate(buffer: Row): Any = buffer.getDouble(0)/buffer.getLong(1)
}
