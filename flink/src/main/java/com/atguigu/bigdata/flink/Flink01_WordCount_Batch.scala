package com.atguigu.bigdata.flink

import org.apache.flink.api.scala._

object Flink01_WordCount_Batch {

    def main(args: Array[String]): Unit = {

        // TODO 使用Flink框架开发 WordCount - 批处理
        // Spark : 1. 获取上下文环境对象
        //         2. 创建RDD
        //         3. RDD 转换 & 行动
        //         4. 环境关闭

        // TODO 1. 创建Flink的上下文执行环境
        val env: ExecutionEnvironment =
            ExecutionEnvironment.getExecutionEnvironment

        // TODO 2. 获取数据
        // 在开发环境中，相对路径从IDEA的主项目中查找的。
        // 绝对路径：不可改变的路径
        // 相对路径：可以改变的路径, 取决于基准路径
        // 默认情况下，开发工具的基准路径就是主项目
        val lineDS: DataSet[String] = env.readTextFile("input")
        // TODO 3. 将一行一行的数据拆分成一个一个单词独立使用，分词
        //         扁平化：将整体拆分成个体来使用的方式

        // 隐式转换
        // 需要导入org.apache.flink.api.scala包下的所有内容
        // import org.apache.flink.api.scala._
        val wordDS: DataSet[String] = lineDS.flatMap(line=>line.split(" "))

        // TODO 4. 将单词进行结构的转换：（Word） => (Word, 1)
        val wordToOneDS: DataSet[(String, Int)] = wordDS.map((_, 1))

        // TODO 5. 将转换结构后的数据按照单词进行分组，聚合数据
        // Aggregate does not support grouping with KeySelector functions, yet.
        val groupDS: GroupedDataSet[(String, Int)] = wordToOneDS.groupBy(0)
        val sumDS: AggregateDataSet[(String, Int)] = groupDS.sum(1)

        // TODO 6. 打印结果
        // org.apache.flink.util.FlinkException: JobManager is shutting down.
        // 这个错误不是错误，而是因为有界流在使用完毕后，自动关闭流
        sumDS.print()

    }
}
