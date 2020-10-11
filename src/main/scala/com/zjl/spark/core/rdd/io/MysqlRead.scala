package com.zjl.spark.core.rdd.io

import java.sql.{DriverManager, ResultSet}

import org.apache.spark.rdd.{JdbcRDD, RDD}
import org.apache.spark.{SPARK_BRANCH, SparkConf, SparkContext}

/**
 * 查看血缘关系和依赖关系
 */
object MysqlRead {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File_RDD")
    //创建Spark上下文环境对象（连接对象）
    val sc: SparkContext = new SparkContext(sparkConf)

    /**
     * rdd的方式访问 mysql数据库
     */
    val url:String="jdbc:mysql://hadoop102:3306/test"
    val user:String="root"
    val password:String="000000"
    val rdd = new JdbcRDD[String](
      sc,
      () => {
        //建立到mysql的连接
        //1.加载驱动
        Class.forName("com.mysql.jdbc.Driver")
        //2.获取连接
        DriverManager.getConnection(url, user, password)
      },
      "select * from user where  id>=?  and id <=?",
      1,
      10,
      2,
      (resultSet: ResultSet) => resultSet.getString(2)
    )
    rdd.collect.foreach(println)
    //关闭连接
    sc.stop()
  }

}
