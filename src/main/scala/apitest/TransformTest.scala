package apitest

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
    val stream1 = dataStream.keyBy("id").sum("temperature")

    stream1.print()

    //
    env.execute("transform test job")

  }
}
