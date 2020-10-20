package com.zjl.streaming.project.app
import com.zjl.streaming.project.bean.AdsInfo
import org.apache.spark.streaming.dstream.DStream

object AreaTopApp  extends App {
  override def doSomething(adsInfoStream: DStream[AdsInfo]): Unit = {
    adsInfoStream.print(1000)
  }
}
