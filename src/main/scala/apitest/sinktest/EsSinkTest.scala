package apitest.sinktest

import java.util

import apitest.SensorReading
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.client.Requests

object EsSinkTest {
  def main(args: Array[String]): Unit = {
    //
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // source, 读入数据
    val inputStream = env.readTextFile("F:\\MyProject\\FlinkProject\\src\\main\\resources\\sensor.txt")
    // Transform 操作
    val dataStream = inputStream.map(data => {
      val dataArray = data.split(",")
      //
      SensorReading(dataArray(0).trim, dataArray(1).trim().toLong, dataArray(2).trim().toDouble)

    })

    // sink
    val httpHosts = new util.ArrayList[HttpHost]()
    httpHosts.add(new HttpHost("192.168.154.101", 9200))

    // 创建一个esSink 的builder
    val esSinkBuilder = new ElasticsearchSink.Builder[SensorReading](
      httpHosts,
      new ElasticsearchSinkFunction[SensorReading] {
        override def process(element: SensorReading, ctx: RuntimeContext, indexer: RequestIndexer): Unit = {
          println("saving data: " + element)
          // 包装成一个Map或者JsonObject
          val json = new util.HashMap[String, String]()
          json.put("sensor_id", element.id)
          json.put("temperature", element.temperature.toString)
          json.put("ts", element.timestamp.toString)

          // 创建index request，准备发送数据
          val indexRequest = Requests.indexRequest()
            .index("sensor")
            .`type`("readingdata")
            .source(json)

          // 利用index发送请求，写入数据
          indexer.add(indexRequest)
          println("data saved.")
        }
      }
    )

    //
    dataStream.addSink( esSinkBuilder.build() )

    //
    env.execute("Es sink")
  }

}
