package com.atguigu.bigdata.flink

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}

object Flink02_WordCount_Bounded {

    def main(args: Array[String]): Unit = {

        // SparkStreaming : 采集数据.start

        // TODO 使用Flink框架开发 WordCount - 有界流
        // TODO 1. 创建Flink的上下文执行环境
        val env: StreamExecutionEnvironment =
            StreamExecutionEnvironment.getExecutionEnvironment

        // 改变并行度
        env.setParallelism(2)

        // TODO 2. 获取数据
        val lineDS: DataStream[String] = env.readTextFile("input")
        // TODO 3. 将一行一行的数据拆分成一个一个单词独立使用，分词
        //         扁平化：将整体拆分成个体来使用的方式
        val wordDS: DataStream[String] = lineDS.flatMap(line=>line.split(" "))

        // TODO 4. 将单词进行结构的转换：（Word） => (Word, 1)
        val wordToOneDS: DataStream[(String, Int)] = wordDS.map((_, 1))
        // TODO 5. 将转换结构后的数据按照单词进行分组，聚合数据
        // 流处理中可以根据key进行分流操作
        val wordKS: KeyedStream[(String, Int), String] = wordToOneDS.keyBy(_._1)
        val resultDS: DataStream[(String, Int)] = wordKS.sum(1)

        // TODO 6. 打印结果
        resultDS.print()

        // TODO 7. Flink是一个流数据处理框架，而且是一个事件驱动的框架
        // 让Flink流处理运行起来
        env.execute()

    }
}
