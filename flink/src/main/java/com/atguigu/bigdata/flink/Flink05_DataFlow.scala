package com.atguigu.bigdata.flink

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}

object Flink05_DataFlow {

    def main(args: Array[String]): Unit = {

        // TODO Flink程序主要分成3部分

        // TODO 任务链：将并行度相同的一对一关系的方法形成一个完整的任务来执行
        // SubTask(N) => Task(1)
        val env: StreamExecutionEnvironment =
            StreamExecutionEnvironment.getExecutionEnvironment

        // 禁用操作链条
        // 会导致操作不会链接在一起形成完整的任务
        // 一个SubTask就是一个Task
        //env.disableOperatorChaining()

        // TODO 1. Source
        val lineDS: DataStream[String] = env.socketTextStream("linux1", 9999)

        // TODO 2. Transform
        // startNewChain方法表示从当前方法开始产生新的任务链条
        val wordDS: DataStream[String] =
            lineDS.flatMap(line=>line.split(" ")).startNewChain()//.setParallelism(1)

        // disableChaining方法表示当前方法不使用链条，是独立的Task
        val wordToOneDS: DataStream[(String, Int)] =
            wordDS.map((_, 1))//disableChaining()

        val wordKS: KeyedStream[(String, Int), String] = wordToOneDS.keyBy(_._1)

        val resultDS: DataStream[(String, Int)] = wordKS.sum(1)

        // TODO 3. Sink
        resultDS.print("sum=")

        env.execute()

    }
}
