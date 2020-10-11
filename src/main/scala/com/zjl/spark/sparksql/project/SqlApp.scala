package com.zjl.spark.sparksql.project

import java.util.Properties

import com.zjl.spark.sparksql.jdbc.JDBCWrite.{password, user}
import org.apache.spark.sql.{DataFrame, SparkSession}

object SqlApp {

  val url="jdbc:mysql://hadoop102:3306/test"
  val user="root"
  val password="000000"
  val dbtable="user"

  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","zjl")
    //1.先创建及SparkSession
    val spark: SparkSession = SparkSession
      .builder()
      .appName("HiveWrite")
      .master("local[*]")
      //添加支持外置hive
      .enableHiveSupport()
      .config("spark.sql.warehouse.dir","hdfs://hadoop102:9000/user/hive/warehouse") //该句为了使创建的表，在hdfs路径上。
      .getOrCreate()

    import spark.implicits._

    val props = new Properties()
    props.put("user",user)
    props.put("password",password)

    spark.udf.register("remark",new CityRemarkUDAF)
    //去执行sql，从hive查询数据
    spark.sql("use spark1015")
    spark.sql(
      """
        |select
        |      ci.*,
        |      pi.product_name,
        |      uva.click_product_id
        |    from user_visit_action uva
        |    join product_info pi on uva.click_product_id=pi.product_id
        |    join city_info ci on uva.city_id=ci.city_id
        |""".stripMargin).createOrReplaceTempView("t1")

    spark.sql(
      """
        |select
        |      area,
        |      product_name,
        |      count(*) count,
        |      remark(city_name) remark
        |
        |    from t1
        |    group by area,product_name
        |""".stripMargin).createOrReplaceTempView("t2")
    spark.sql(
      """
        |
        |    select
        |      area,
        |      product_name,
        |      count,
        |      remark,
        |      rank() over(partition by area order by count desc) rk
        |    from t2
        |""".stripMargin).createOrReplaceTempView("t3")



    spark.sql(
      """
        | select
        |      area,
        |      product_name,
        |      count,
        |      remark
        |    from t3
        |    where rk<=3
        |""".stripMargin)
      .coalesce(1) //数据汇总到一个分区写入mysql，可以保证顺序
      .write
      .mode("overwrite")
      .jdbc("jdbc:mysql://hadoop102:3306/test?useUnicode=true&characterEncoding=utf8","sql1015",props)

    //此处写入到mysql中会产生乱码问题。  解决方式：jdbc:mysql://hadoop102:3306/test?useUnicode=true&characterEncoding=utf8
    //把结果写入到mysql里
    spark.close()
  }

  /*
  这里的热门商品是从点击量的维度来看的，计算各个区域前三大热门商品，并备注上每个商品在主要城市中的分布比例，超过两个城市用其他显示。
    例如：
    地区	商品名称	点击次数	城市备注
    华北	商品A	100000	北京21.2%，天津13.2%，其他65.6%
    华北	商品P	80200	北京63.0%，太原10%，其他27.0%
    华北	商品M	40000	北京63.0%，太原10%，其他27.0%

    ---------------------------------------------

    自定义聚合函数：
      聚合函数，只需要接受一个城市名
      每收到一个城市名，可以给这个城市统计一次

    ---------------------------------------------
    1.先把需要的字段查出来  t1

    select
      ci.*,
      pi.product_name,
      uva.click_product_id
    from user_visit_action uva
    join product_info pi on uva.click_product_id=pi.product_id
    join city_info ci on uva.city_id=ci.city_id

    2.按照地区和商品名称聚合 t2
    select
      area,
      product_name,
      count(*) count
    from t1
    group by area,product_name

    3.按照地区进行分组开窗  排序  开窗函数  t3  (rank（1 2 2 4 4 ...） row_number(1 2 3 4...) dense_rank(1 2 2 3 4)

    select
      area,
      product_name,
      count,
      rank() over(partition by area order by count desc) rk
    from t2

    4.过滤出名次小于等于3 的
    select
      area,
      product_name,
      count
    from t3
    where rk<=3

    -------------------------------------------------------

    CREATE TABLE `user_visit_action`(
      `date` string,
      `user_id` bigint,
      `session_id` string,
      `page_id` bigint,
      `action_time` string,
      `search_keyword` string,
      `click_category_id` bigint,
      `click_product_id` bigint,
      `order_category_ids` string,
      `order_product_ids` string,
      `pay_category_ids` string,
      `pay_product_ids` string,
      `city_id` bigint)
    row format delimited fields terminated by '\t';
    load data local inpath '/opt/module/datas/user_visit_action.txt' into table user_visit_action;

    CREATE TABLE `product_info`(
      `product_id` bigint,
      `product_name` string,
      `extend_info` string)
    row format delimited fields terminated by '\t';
    load data local inpath '/opt/module/datas/product_info.txt' into table product_info;

    CREATE TABLE `city_info`(
      `city_id` bigint,
      `city_name` string,
      `area` string)
    row format delimited fields terminated by '\t';
    load data local inpath '/opt/module/datas/city_info.txt' into table city_info;
   */
}
