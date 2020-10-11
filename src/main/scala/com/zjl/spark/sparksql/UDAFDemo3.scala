package com.zjl.spark.sparksql

import org.apache.spark.sql.expressions.{Aggregator, MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Encoders, Row, SparkSession, TypedColumn}

case class Dog(name:String,age:Int)
case class AgeAve(sum:Int,count:Int){
  def avg=sum.toDouble/count
}
object UDAFDemo3 {
  /**
   * 强类型 聚合函数
   * @param args
   */
  def main(args: Array[String]): Unit = {
    //在sql中，集合函数如何使用
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("UDAFDemo")
      .getOrCreate()

    import spark.implicits._
    val ds: Dataset[Dog] = List(Dog("大黄", 6), Dog("小黄", 2), Dog("中黄", 4)).toDS()
    //强类型的使用方法
    val avg: TypedColumn[Dog, Double] = new MyAvg2().toColumn.name("avg")
    var result:Dataset[Double]=ds.select(avg)
    result.show()

    spark.close()

  }

}

class MyAvg2 extends Aggregator[Dog,AgeAve,Double] {
  //对缓冲区进行初始化
  override def zero: AgeAve = AgeAve(0,0)

  //聚合（分区内聚合）
  override def reduce(b: AgeAve, a: Dog): AgeAve = a match {
    case Dog(name,age)=>AgeAve(b.sum+a.age,b.count+1)
      //如果是null，则原封不动返回
    case _=>b
  }

  //分区间的聚合
  override def merge(b1: AgeAve, b2: AgeAve): AgeAve ={
    AgeAve(b1.sum+b2.sum,b1.count+b2.count)
  }

  //返回最终的值
  override def finish(reduction: AgeAve): Double = reduction.avg

  //对缓冲区进行编码
  override def bufferEncoder: Encoder[AgeAve] = Encoders.product //如果是样例，就直接返回这个编码器就行了

  //对返回值进行编码
  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}
