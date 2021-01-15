package source

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment


//从文件读取数据
object FileReading {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream2 = env.readTextFile("C:\\Users\\Administrator\\Desktop\\Flink\\src\\main\\resources\\hello.txt")
    stream2.print("file_line:").setParallelism(1)
    env.execute()
  }
}
