package apitest.sinktest

import java.util.Properties

import apitest.SensorReading
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}

object KafkaSinkTest {
  def main(args: Array[String]): Unit = {
    //
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // kafka 配置
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "192.168.154.101:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")

    // source, 读入数据
//    val inputStream = env.readTextFile("F:\\MyProject\\FlinkProject\\src\\main\\resources\\sensor.txt")
    // 从Kafka读取数据，处理后，再写入到kafka
    val inputStream = env.addSource(new FlinkKafkaConsumer011[String]("sensor", new SimpleStringSchema(), properties))

    // Transform 操作
    val dataStream = inputStream.map(data => {
      val dataArray = data.split(",")
      //
      SensorReading(dataArray(0).trim, dataArray(1).trim().toLong, dataArray(2).trim().toDouble)
        // 转成String,方便输出
        .toString
    })

    // sink: kafka
    dataStream.addSink(new FlinkKafkaProducer011[String]("sinkTest",
      new SimpleStringSchema(), properties))
    dataStream.print("kafka sink")

    //
    env.execute("Kafka sink")
  }
}
