package apitest

import java.util.Random

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._

// 定义传感器数据样例类
case class SensorReading(id: String, timestamp: Long, temperature: Double)

object SourceTest {
  def main(args: Array[String]): Unit = {
    //
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 1. 从集合中读取数据
    fromList(env)

    // 2. 从文件中读取数据
    fromFile(env)

    // 3. 从kafka中读取数据

    // 4. 自定义数据源
    fromCustomDefine(env)

    // 提交执行
    env.execute("Source test")
  }

  // 从集合中读取数据
  def fromList(env: StreamExecutionEnvironment) = {
    val stream1 = env.fromCollection(List(
      SensorReading("sensor_1", 1547718199, 35.80018327300259),
      SensorReading("sensor_6", 1547718201, 15.402984393403084),
      SensorReading("sensor_7", 1547718202, 6.720945201171228),
      SensorReading("sensor_10", 1547718205, 38.101067604893444)
    ))

    stream1.print("stream1").setParallelism(1).setParallelism(6)
  }

  // 从文件中读取数据
  def fromFile(env: StreamExecutionEnvironment): Unit = {
    val stream2 = env.readTextFile("F:\\MyProject\\FlinkProject\\src\\main\\resources\\sensor.txt")
    stream2.print("stream2").setParallelism(1).setParallelism(6)
  }

  // 自定义数据源中读取
  def fromCustomDefine(env: StreamExecutionEnvironment): Unit ={
    val stream4 = env.addSource(new SensorSource())
    stream4.print("stream4")
  }

}

//
class SensorSource() extends SourceFunction[SensorReading]{
  // 定义一个flag：表示数据源是否还在正常运行
  var running: Boolean = true

  // 生成数据
  override def run(sourceContext: SourceFunction.SourceContext[SensorReading]): Unit = {
    // 创建一个随机数发生器
    val rand = new Random()

    // 随机初始换生成10个传感器的温度数据，之后在它基础随机波动生成流数据
    var curTemp = 1.to(10).map(
      i => ( "sensor_" + i, 60 + rand.nextGaussian() * 20 )
    )

    // 无限循环生成流数据，除非被cancel
    while(running){
      // 更新温度值
      curTemp = curTemp.map(
        // 在原来数据的基础上，随机加一个高斯数
        t => (t._1, t._2 + rand.nextGaussian())
      )
      // 获取当前的时间戳
      val curTime = System.currentTimeMillis()
      // 包装成SensorReading，输出
      curTemp.foreach(
        // 使用 sourceContext 输出
        t => sourceContext.collect( SensorReading(t._1, curTime, t._2) )
      )
      // 间隔100ms
      Thread.sleep(100)
    }
  }

  // 取消生成
  override def cancel(): Unit = {
    running = false
  }
}
