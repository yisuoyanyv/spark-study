package com.zjl.spark.core.rdd.io

import java.sql.{DriverManager, ResultSet}

import org.apache.spark.rdd.{JdbcRDD, RDD}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 查看血缘关系和依赖关系
 */
object MysqlWrite {
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

    //先有数据，然后去写
//    val rdd: RDD[(Int, String)] = sc.makeRDD((20, "zs") :: (30, "lisi") :: (35, "ww") :: Nil)
    val rdd: RDD[(Int, String)] = sc.makeRDD(1 to 10000).map(x => (x, "zjl" + x))
    val sql="insert into user1 values(?,?)"

    //连接数过多，每个元素都需要建立一个到mysql的连接，实际不能用
   /* rdd.foreach{
      case(age,name)=>{
         Class.forName("com.mysql.jdbc.Driver")
        val conn=DriverManager.getConnection(url, user, password)
        val ps=conn.prepareStatement(sql)
        ps.setInt(1,age)
        ps.setString(2,name)
        ps.execute()
        ps.close()
        conn.close()
      }
    }*/

    //应该每个分区建立一个到MySQL的连接
    /*rdd.foreachPartition{
      it=>{
        Class.forName("com.mysql.jdbc.Driver")
        val conn=DriverManager.getConnection(url, user, password)
        //一个元素写一次，效率不够高
        it.foreach{
          case(age,name)=>
            val ps=conn.prepareStatement(sql)
            ps.setInt(1,age)
            ps.setString(2,name)
            ps.execute()
            ps.close()
        }

        conn.close()
      }
    }*/

    rdd.foreachPartition{
      it=>{
        Class.forName("com.mysql.jdbc.Driver")
        val conn=DriverManager.getConnection(url, user, password)
        //一次写一个批次
        val ps=conn.prepareStatement(sql)
        var count=0
        it.foreach{
          case(age,name)=>
            ps.setInt(1,age)
            ps.setString(2,name)
            ps.addBatch()
            count+=1
            if(count % 100 == 0) {
              ps.executeBatch();
              count=0
              Thread.sleep(200)
            }
        }
        //必须要有
        ps.executeBatch()
        ps.close()
        conn.close()
      }
    }

    //关闭连接
    sc.stop()
  }

}
