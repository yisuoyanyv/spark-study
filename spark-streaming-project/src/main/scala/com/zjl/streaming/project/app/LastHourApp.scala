package com.zjl.streaming.project.app
import com.zjl.streaming.project.bean.AdsInfo
import com.zjl.streaming.project.util.RedisUtil
import org.apache.spark.streaming.{Minutes, Seconds}
import org.apache.spark.streaming.dstream.DStream
import org.json4s.jackson.JsonMethods
import redis.clients.jedis.Jedis

object LastHourApp  extends App {
  override def doSomething(adsInfoStream: DStream[AdsInfo]): Unit = {
    //1.先把窗口分好
    adsInfoStream.window(Minutes(60),Seconds(3))
    //2.按照广告分钟  进行聚合
      .map(info=>((info.adsId,info.hmString),1))
      .reduceByKey(_+_)
     //3.再按照广告分组，把这个广告下所有的分钟记录放在一起
      .map{
        case ((ads,hm),count)=>(ads,(hm,count))
      }
      .groupByKey()
      //4.把结果写入redis中
      .foreachRDD(rdd=>{
        rdd.foreachPartition(it=>{
          if (it.nonEmpty) {// 只是判断是否有下一个元素，指针不会跳过这个元素 千万别用size判断
            //1.先建立到redis连接
            val client: Jedis = RedisUtil.getClient

            //2.写元素到redis
            //2.1一个一个的写（昨天）
            //2.2批次写入\
            import org.json4s.JsonDSL._

            val key="last:ads:hour:count"
            val map=it.toMap.map{
              case(adsId,it)=>(adsId,JsonMethods.compact(JsonMethods.render(it)))
            }
            //把scala集合转换为java集合
            import scala.collection.JavaConversions._
            client.hmset(key,map)

            //3.关闭redis（用的是连接池，实际是把连接归还给连接池
            client.close()
          }



        })
      })

  }

  /**
   * 统计各广告最近1小时内的点击量趋势：
   * 各广告最近1小时内各分钟的点击量，每6秒统计一次
   *
   * 1.各广告    -》 按照（广告，分钟）分组
   * 2.最近1小时，每6秒统计一次    --》窗口：窗口长度1小时，窗口的滑动步长 5s
   *
   * ---
   *
   * 1.先把窗口分好
   *
   * 2.按照广告分钟  进行聚合
   * 3.再按照广告分组，把这个广告下所有的分钟记录放在一起
   * 4.把结果写入redis中
   *
   * 写到redis的时候，数据类型
   * 1.
   *      key        value
   *    广告的id      json字符串每分钟的点击量
   *
   * 2.
   *    key                   value
   * "last:ads:hour:count"    hash
   *                          field          value
   *                          adsId          json字符串
   *                          ”1“           {”09：24“：100，”09：25“：110}
   *
   */
}
