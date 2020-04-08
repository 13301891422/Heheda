package com.atguigu.bigdata.flink

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}

object Flink06_API_Env {

    def main(args: Array[String]): Unit = {

        // TODO 上下文环境

        // 批处理的上下文环境
        // DataSet
        val batchEnv = ExecutionEnvironment.getExecutionEnvironment
        // 设定并行度
        batchEnv.setParallelism(1)

        // 流处理的上下文环境
        // DataStream
        val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment
        streamEnv.setParallelism(1)


    }
}
