package apitest.sinktest

import apitest.SensorReading
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

object RedisSinkTest {
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
    val conf = new FlinkJedisPoolConfig.Builder()
      .setHost("192.168.154.101")
      .setPort(6379)
      .build()

    // sink
    dataStream.addSink(new RedisSink(conf, new MyRedisMapper()))
    // 打开redis客户端，查看数据是否写入成功：
    //  keys *
    // hgetall sensor_temperature
    dataStream.print("Write to redis")

    //
    env.execute("Redis sink")
  }

}

class MyRedisMapper() extends RedisMapper[SensorReading] {
  // 定义保存数据到redis的命令
  override def getCommandDescription: RedisCommandDescription = {
    // 把传感器id和温度值保存成哈希表 HSET key field value
    new RedisCommandDescription(RedisCommand.HSET, "sensor_temperature")
  }

  // 定义保存到redis的value
  override def getValueFromData(t: SensorReading): String = {
    t.temperature.toString
  }

  // 定义保存到redis的key
  override def getKeyFromData(t: SensorReading): String = {
    t.id
  }
}


