package wordcount

import org.apache.flink.api.scala._

object WordCount {
  def main(args: Array[String]): Unit = {
    // 定义一个执行环境
    val env = ExecutionEnvironment.getExecutionEnvironment
    // 从文件中读数据
    val inputPath = "F:\\MyProject\\FlinkProject\\src\\main\\resources\\hello.txt"
    val inputDataSet  = env.readTextFile(inputPath)

    // 切分成word
    val wordCountSet = inputDataSet.flatMap(_.split(" "))
        .map((_,1)).groupBy(0).sum(1)
    wordCountSet.print()

  }
}
