package com.zjl.streaming.project.app
import com.zjl.streaming.project.bean.AdsInfo
import com.zjl.streaming.project.util.RedisUtil
import org.apache.spark.streaming.dstream.DStream
import org.json4s.jackson.JsonMethods
import redis.clients.jedis.Jedis

object AreaTopApp  extends App {
  override def doSomething(adsInfoStream: DStream[AdsInfo]): Unit = {
    val dayAreaGrouped: DStream[((String, String), Iterable[(String, Int)])] = adsInfoStream.map(info => ((info.dayString, info.area, info.adsId), 1))
      .updateStateByKey((seq: Seq[Int], opt: Option[Int]) => {
        Some(seq.sum + opt.getOrElse(0)) //先计算每天每地区每广告的点击量
      })
      .map { //map处理 （day,area)作为key
        case ((day, area, ads), count) => ((day, area), (ads, count))
      }
      .groupByKey()

    // 每组内进行排序取前3

    val result: DStream[((String, String), List[(String, Int)])] = dayAreaGrouped.map {
      case (key, it) =>
        (key, it.toList.sortBy(-_._2).take(3))
    }
    result.print(100)

   /* result.foreachRDD(rdd=>{
      rdd.foreachPartition(it=>{
        //1.建立到redis的连接

        val client: Jedis = RedisUtil.getClient

        //2.写数据到redis
        it.foreach{
          case ((day,area),adsCountList)=>
            val key="area:ads:count" +day
            val field=area
            //把集合转换成json字符串 json4s
            //专门用于把集合转成字符串（样例类不行）
            import org.json4s.JsonDSL._
            val value:String=JsonMethods.compact(JsonMethods.render(adsCountList))
            client.hset(key,field,value)
        }

        //3.关闭到redis的连接
        client.close() //其实是把客户端还给连接池
      })
    })*/

    import com.zjl.streaming.project.util.RealUtil._
    result.saveToRedis


  }

  /**
   * 每天每地区热门广告
   *
   * 1.先计算每天每地区每广告的点击量
   * ((day,area,ads),1)=> updatestateByKey
   *
   * 2.按照每天每地区分组
   *
   * 3.每组内排序，取前3
   *
   * 4.把数据写入到redis
   *
   *
   * 数据类型：
   *
   * k-v形式数据库  （no sql 数据）
   * k:都是字符串
   * v的数据类型：
   *  5大数据类型
   *    1.String
   *    2.set 不重复
   *    3. list 允许重复
   *    4. hash   map,存的是 field-value
   *    5. zset
   *
   *
   * ((2020-10-20,华东),List((1,26), (4,17), (2,15)))
   * ((2020-10-20,华南),List((3,22), (2,15), (4,15)))
   * ((2020-10-20,华北),List((5,23), (4,22), (2,20)))
   * ((2020-10-20,华中),List((1,7), (4,6), (5,6)))
   *
   * ----
   * 选择什么类型的数据：
   * 每天一个key
   *
   * key                                                value
   * “area:ads:count” + day                             hash
   *                                                    field    value
   *                                                    area     json字符串
   *                                                    “华中"    {3:14,1:12,2:8}
   */
}
