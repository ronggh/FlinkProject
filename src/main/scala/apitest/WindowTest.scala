package apitest

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.{AssignerWithPeriodicWatermarks, AssignerWithPunctuatedWatermarks}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time

object WindowTest {
  def main(args: Array[String]): Unit = {
    //
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 设置事件时间
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getConfig.setAutoWatermarkInterval(500)

    // source, 读入数据
    //    val inputStream = env.readTextFile("F:\\MyProject\\FlinkProject\\src\\main\\resources\\sensor.txt")
    // nc -lk 7777 开启端口
    val inputStream = env.socketTextStream("192.168.154.101", 7777)
    // Transform 操作
    val dataStream = inputStream.map(data => {
      val dataArray = data.split(",")
      //
      SensorReading(dataArray(0).trim, dataArray(1).trim().toLong, dataArray(2).trim().toDouble)
    })
      //    .assignAscendingTimestamps(_.timestamp * 1000L)
      .assignTimestampsAndWatermarks(new MyAssigner())


    // 统计10秒内的最小温度
    val minTempPerWindowStream = dataStream.map(data => (data.id, data.temperature))
      .keyBy(_._1)
      .timeWindow(Time.seconds(10)) // 开时间窗口
      .reduce((data1, data2) => (data1._1, data1._2.min(data2._2))) // 取最小值
    minTempPerWindowStream.print("min temp")
    dataStream.print("input stream")

    //
    env.execute("window api test")
  }
}

//
class MyAssigner() extends AssignerWithPeriodicWatermarks[SensorReading] {
  // 定义固定延迟为3秒
  val bound: Long = 3 * 1000L
  // 定义当前收到的最大的时间戳
  var maxTs: Long = Long.MinValue

  override def getCurrentWatermark: Watermark = {
    new Watermark(maxTs - bound)
  }

  override def extractTimestamp(element: SensorReading, previousElementTimestamp: Long): Long = {
    maxTs = maxTs.max(element.timestamp * 1000L)
    element.timestamp * 1000L
  }
}

//
class MyAssigner2() extends AssignerWithPunctuatedWatermarks[SensorReading] {
  val bound: Long = 1000L

  override def checkAndGetNextWatermark(lastElement: SensorReading, extractedTimestamp: Long): Watermark = {
    if (lastElement.id == "sensor_1") {
      new Watermark(extractedTimestamp - bound)
    } else {
      null
    }
  }

  override def extractTimestamp(element: SensorReading, previousElementTimestamp: Long): Long = {
    element.timestamp * 1000L
  }
}
