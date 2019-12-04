package apitest

import org.apache.flink.api.common.functions.{FilterFunction, RichMapFunction}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._

object TransformTest {
  def main(args: Array[String]): Unit = {
    //
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 设定一个全局的并行度
    env.setParallelism(1)

    // 读入数据
    val inputStream = env.readTextFile("F:\\MyProject\\FlinkProject\\src\\main\\resources\\sensor.txt")
    // Transform操作
    val dataStream = inputStream.map(data => {
      val dataArray = data.split(",")
      SensorReading(dataArray(0).trim, dataArray(1).trim().toLong, dataArray(2).trim().toDouble)
    })

    // 1. 聚合操作
    val aggStream = dataStream.keyBy("id")
      //.sum("temperature")
      .reduce((x, y) => SensorReading(x.id, x.timestamp + 1, y.temperature + 10))
    aggStream.print("aggStream")

    // 2. 分流，根据温度是否大于30度划分
    val splitStream = dataStream
      .split(sensorData => {
        if (sensorData.temperature > 30) Seq("high") else Seq("low")
      })

    val highTempStream = splitStream.select("high")
    val lowTempStream = splitStream.select("low")
    val allTempStream = splitStream.select("high", "low")
    highTempStream.print("high")
    lowTempStream.print("low")
    allTempStream.print("all")

    // 3. 合并两条流
    // 3.1 connect和coMap,connect只能合并两条流，数据结构可以不一样
    val warningStream = highTempStream.map(sensorData => (sensorData.id, sensorData.temperature))
    val connectedStreams = warningStream.connect(lowTempStream)

    val coMapStream = connectedStreams.map(
      warningData => (warningData._1, warningData._2, "high temperature warning"),
      lowData => (lowData.id, lowData.temperature, "healthy")
    )
    coMapStream.print("coMapStream")

    // 3.2 union，可以合并多条流，数据结构必须要一样
    val unionStream = highTempStream.union(lowTempStream)
    unionStream.print("unionStream")

    // 4. 自定义UDF函数类
    val udfStream = dataStream.filter(new MyFilter())
    udfStream.print("udfStream")

    //
    env.execute("transform test job")

  }
}

// 自定义函数过滤类：将包含 sensor_1 的过滤出来
class MyFilter() extends FilterFunction[SensorReading]{
  override def filter(value: SensorReading): Boolean = {
    value.id.startsWith("sensor_1")
  }
}

// 富函数类
class MyMapper() extends RichMapFunction[SensorReading, String]{
  //
  override def map(value: SensorReading): String = {
    value.id
  }

  // 其它方法 open,close等
  override def open(parameters: Configuration): Unit = super.open(parameters)
}
