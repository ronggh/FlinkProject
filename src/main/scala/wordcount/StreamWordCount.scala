package wordcount

import org.apache.flink.streaming.api.scala._

object StreamWordCount {
  def main(args: Array[String]): Unit = {
    // 创建一个流式处理环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 读取流式文件
    val dataStream = env.socketTextStream("127.0.0.1", 7777)
    // 对每条数据进行处理
    val steamDateSet = dataStream.flatMap(_.split(" "))
      .filter(_.nonEmpty)
      .map((_, 1))
      .keyBy(0)
      .sum(1)

    steamDateSet.print()

    // 启动任务
    env.execute("stream word count job")

  }

}
