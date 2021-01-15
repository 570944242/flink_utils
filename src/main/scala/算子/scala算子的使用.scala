package 算子

import java.util.Properties

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

object scala算子的使用 {

  // 定义相关的配置
  val properties = new Properties()
  properties.setProperty("bootstrap.servers", "114.67.202.170:9092")
  properties.setProperty("group.id", "test")
  properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  properties.setProperty("auto.offset.reset", "earliest")
  val kafkaSource = new FlinkKafkaConsumer[String]("test", new SimpleStringSchema(), properties)

  def main(args: Array[String]): Unit = {

    /*
    {
      "name": "钟伟",
      "country": "斐济",
      "city": "丽市",
      "address": "海南省重庆县崇文谭街S座 629005",
      "postcode": "567019",
      "latitude": "10.6857545",
      "longitude": "-110.529257",
      "company": "思优信息有限公司",
      "create_time": "2021-01-15",
      "time": "1900-01-01 05:25:49",
      "ipv4": "61.26.135.248",
      "uri_path": "wp-content/category/wp-content",
      "uri": "https://www.yan.cn/list/register.jsp",
      "tld": "cn",
      "user_name": "ryin",
      "mac_address": "2d:62:33:78:5b:b9",
      "safe_email": "xia69@example.com",
      "company_email": "xiuyingcao@min.cn",
      "job": "项目主管",
      "words": ["她的", "喜欢", "企业"],
      "phone_number": "14581800870",
      "randint": 45,
      "order_pay": 715.42373724646,
      "is_show": false
    }
    */
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env
      .addSource(kafkaSource)
    val JSONNobStream: DataStream[JSONObject] = stream.map((line: String) => {
      val JsonObj: JSONObject = JSON.parseObject(line)
      JsonObj
    })
    //过滤掉金额小于750的订单
    val filterStream = JSONNobStream.filter(fun = (JsonObj: JSONObject) => {
      val order_pay = JsonObj.getBigDecimal("order_pay")
      val isFl: Boolean = order_pay > 750
      isFl
    })

    filterStream.print()
    env.execute()


  }
}
