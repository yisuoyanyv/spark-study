package com.zjl.spark.core.rdd.operator.transfer

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark20_RDD_Operator7 {
  def main(args: Array[String]): Unit = {
    //TODO Spark - RDD -算子（方法）

    //glom =>将每个分区的数据转换为数组
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File_RDD")
    //创建Spark上下文环境对象（连接对象）
    val sc: SparkContext = new SparkContext(sparkConf)

    val dataRDD = sc.makeRDD(List(1, 2, 3, 4, 5, 6), 3)

    //TODO 分组
    //groupBy方法可以根据指定的规则进行分组，指定的规则的返回值就是分组的key
    //groupyBy方法的返回值为元组
    //   元组中的一个元素，表示分组的key
    //   元组的第二个元素，表示相同的key的数据形成的可迭代的集合
    //groupBy方法执行完毕后，会将数据进行分组操作，但是分区时不会改变的
    //   不同的组的数据会打乱在不同的分区中

    //如果将上游的分区数据打乱重新组合到下游的分区中，那么这个操作称之shuffle

    //如果数据被打乱重新组合，那么数据就可能出现不均匀的情况
    //groupBy方法会导致数据不均匀，产生shuffle操作。如果想改变分区，可以传递参数
    val rdd: RDD[(Int, Iterable[Int])] = dataRDD.groupBy(
      num => {
        num % 2
      }, 2
    )

    //glom=>分区转换为一个array
    //    println("分组后的数据分区的数量="+rdd.glom().collect().length)
    //   rdd.collect().foreach{
    //     case (key,list)=>{
    //       println("key:"+key+"  list["+list.mkString(",")+"]")
    //     }
    //   }

    rdd.saveAsTextFile("output")

    sc.stop()


  }
}
