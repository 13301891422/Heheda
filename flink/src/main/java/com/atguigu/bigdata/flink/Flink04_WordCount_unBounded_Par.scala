package com.atguigu.bigdata.flink

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}

object Flink04_WordCount_unBounded_Par {

    def main(args: Array[String]): Unit = {

        val env: StreamExecutionEnvironment =
            StreamExecutionEnvironment.getExecutionEnvironment

        // TODO 并行度设置
        // 每一个方法都可以单独设定并行度，需要考虑数据量的操作
        // 1. 如果方法单独设定并行度，那么使用优先级最高
        // 2. 然后才是全局并行度设置
        // 3. 如果程序代码中没有设定并行度，会采用提交的命令行参数设定并行度
        // 4. 如果没有命令行参数，那么会采用集群启动时的默认并行度parallelism.default

        // TODO 并行度和资源Slot的关系
        // Slot表示可用资源（核）
        // 1. 独立部署模式：如果设定并行度大于可用资源，那么程序会一直阻塞，需要等待资源
        //

        // TODO Slot : 插槽
        val lineDS: DataStream[String] = env.socketTextStream("linux1", 9999)

        val wordDS: DataStream[String] = lineDS.flatMap(line=>line.split(" "))

        val wordToOneDS: DataStream[(String, Int)] = wordDS.map((_, 1))

        // keyBy可以分流数据到不同的分区
        val wordKS: KeyedStream[(String, Int), String] = wordToOneDS.keyBy(_._1)

        val resultDS: DataStream[(String, Int)] = wordKS.sum(1)

        // 为了能够区分打印的内容，可以增加打印格式的前缀
        wordKS.print("ks=")
        resultDS.print("sum=")

        env.execute()

    }
}
