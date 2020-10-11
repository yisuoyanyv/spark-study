package com.zjl.spark.core.rdd.operator.transfer

import org.apache.spark.{SparkConf, SparkContext}

object Spark47_RDD_Operator29 {
  def main(args: Array[String]): Unit = {
    //TODO Spark 序列化

    /**
     * - 为什么要序列化
     *   因为在Spark程序中，算子相关的操作在Excutor上执行，算子之外的代码在Driver端执行，
     *   在执行有些算子的时候，需要使用到Driver里面定义的数据，这就涉及到了跨进程或者跨节点之间的通讯，
     *   所以要求传递给Executor中的数组所属的类型必须实现Serializable接口
     *   -如何判断是否实现了序列号接口
     *     在作业job提交之前，其中有一行代码 val cleanF = sc.clean(f),用于进行闭包检查
     *     之所以叫闭包检查，是因为在当前函数的内部访问了外部函数的变量，属于闭包的形式。
     *     如果算子的参数是函数的形式，都会存在这种情况
     */
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File_RDD")
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
