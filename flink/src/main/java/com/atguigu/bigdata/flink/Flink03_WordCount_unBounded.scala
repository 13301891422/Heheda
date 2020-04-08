package com.atguigu.bigdata.flink

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}

object Flink03_WordCount_unBounded {

    def main(args: Array[String]): Unit = {

        // TODO 使用Flink框架开发 WordCount - 无界流
        val env: StreamExecutionEnvironment =
            StreamExecutionEnvironment.getExecutionEnvironment
        // 改变并行度
        // 程序中如果设定了并行度，可以覆盖启动配置
        // env可以设定全局并行度配置
        //env.setParallelism(1)

        // TODO 从Socket中获取网络数据
        // 每一个方法都可以单独设定并行度，需要考虑数据量的操作
        // 如果方法单独设定并行度，那么使用优先级最高
        // 然后才是全局并行度设置
        val lineDS: DataStream[String] = env.socketTextStream("linux1", 9999)

        val wordDS: DataStream[String] = lineDS.flatMap(line=>line.split(" "))

        val wordToOneDS: DataStream[(String, Int)] = wordDS.map((_, 1))//.setParallelism(2)

        // keyBy可以分流数据到不同的分区
        val wordKS: KeyedStream[(String, Int), String] = wordToOneDS.keyBy(_._1)

        val resultDS: DataStream[(String, Int)] = wordKS.sum(1)

        // 为了能够区分打印的内容，可以增加打印格式的前缀
        //wordKS.print("ks=")
        resultDS.print("sum=")

        env.execute()

    }
}
