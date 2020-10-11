package com.zjl.spark.core.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

object CustomAcc {
  def main(args: Array[String]): Unit = {
    //TODO Spark - 累加器

    //创建Spark运行配置对象
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File_RDD")

    //创建Spark上下文环境对象（连接对象）
    val sc: SparkContext = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4,5,6))


    //先注册自定义的累加器
    val acc = new MyIntAcc
    sc.register(acc,"first")

    val resultRDD: RDD[Int] = rdd.map {
      x => {
        acc.add(1)
        x
      }
    }

    resultRDD.collect
    println(acc.value)

    sc.stop()


  }
}

//对什么值进行累加  累加器最终的值
class MyIntAcc extends AccumulatorV2[Int,Int] {

  private var sum=0
  //判“零” ，对缓存区值进行判“零”
  override def isZero: Boolean = sum==0

  //把当前的累积复制为一个新的累加器
  override def copy(): AccumulatorV2[Int, Int] = {
    val acc=new MyIntAcc
    acc.sum=sum
    acc
  }

  //重置累加器 （就是把缓冲区的值重置为“零”）
  override def reset(): Unit = sum=0

  //真正的累加方法（分区内的累加）
  override def add(v: Int): Unit = sum+=v

  //分区间的合并 把other的sum合并到this的sum中
  override def merge(other: AccumulatorV2[Int, Int]): Unit =
    other match {
      case acc:MyIntAcc => this.sum += acc.sum
      case _ => this.sum += 0
    }


  //返回累积后的最终值
  override def value: Int = sum
}