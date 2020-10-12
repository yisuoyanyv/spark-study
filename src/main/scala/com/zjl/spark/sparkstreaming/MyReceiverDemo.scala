package com.zjl.spark.sparkstreaming

import java.io.{BufferedReader, InputStream, InputStreamReader}
import java.net.Socket

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.receiver.Receiver

object MyReceiverDemo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("wordCount2").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(3))

    val sourceStream: ReceiverInputDStream[String] = ssc.receiverStream(new MyReceiver("hadoop102", 9999))

    sourceStream
      .flatMap(_.split(" "))
      .map((_,1))
      .reduceByKey(_+_)
      .print()

    ssc.start()

    ssc.awaitTermination()
  }

}

//接收器从socket接收数据
class MyReceiver( host:String, port:Int) extends Receiver[String](StorageLevel.MEMORY_ONLY){
  var socket:Socket=_
  var reader:BufferedReader=_
  override def onStart(): Unit = {
    //连接socket,去读取数据 socket 编程
    runInThread{
      try{
        socket = new Socket(host, port)
        reader = new BufferedReader( new InputStreamReader(socket.getInputStream, "utf-8"))
        var line: String = reader.readLine()
        // 当对方发送一个流结束标志的时候，会收到null
        while (line != null && socket.isConnected){
          store(line)
          line=reader.readLine()  //如果流中没有数据，这里会一直阻塞
        }
      }catch {
        case e=>println(e.getMessage)
      }finally {
        restart("重启接收器")
        //立即调用 onStop 方法，再调用OnStart方法
      }
    }

  }

  //在一个子线程中去调用
  def runInThread(op: =>Unit) = {
    new Thread(){
      override def run(): Unit = op
    }.start()

  }

  //释放资源
  override def onStop(): Unit = {
    if(socket != null) socket.close()
    if(reader != null) reader.close()

  }
}