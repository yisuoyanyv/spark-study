package com.zjl.spark.core.rdd.operator.transfer

import org.apache.spark.{SparkConf, SparkContext}

object Spark48_RDD_Operator30 {
  def main(args: Array[String]): Unit = {
    //TODO Spark 序列化


    val sparkConf = new SparkConf()
          .setMaster("local[*]")
          .setAppName("File_RDD")
      //替换默认的序列化机制
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      //注册需要使用kryo序列化的自定类
      .registerKryoClasses(Array(classOf[User]))
    //创建Spark上下文环境对象（连接对象）
    val sc: SparkContext = new SparkContext(sparkConf)

    val user1 = new User
    user1.name="zhangsan"
    val user2 = new User
    user2.name="lisi"

    val rdd = sc.makeRDD(List(user1,user2))

    //分布式打印
    rdd.foreach(user=>println(user))
    sc.stop()

  }

  class User extends Serializable {
    var name:String=_

    override def toString: String = s"User($name)"
  }

  
}
