package single

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
/*
使用方式：
 1.服务器安装nc
 2.服务器启动nc： nc -lk 8632 ///nc -v localhost 8632
 3.运行代码
 4.服务器输入字符串
*/

object StreamWorldCount {
  def main(args: Array[String]): Unit = {


    // 创建流处理环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 接收socket文本流
    val textDstream: DataStream[String] = env.socketTextStream("120.24.59.45", 8632)

    // flatMap和Map需要引用的隐式转换
    import org.apache.flink.api.scala._
    val dataStream: DataStream[(String, Int)] = textDstream.flatMap(_.split("\\s")).filter(_.nonEmpty).map((_, 1)).keyBy(0).sum(1)

    dataStream.print().setParallelism(1)

    // 启动executor，执行任务
    env.execute("Socket_stream_word_count")
  }
}
