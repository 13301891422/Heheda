package com.atguigu.bigdata.flink

import org.apache.flink.api.common.functions.{AggregateFunction, ReduceFunction}
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time

object Flink35_API_Window_Function1 {

    def main(args: Array[String]): Unit = {

        // TODO  API - Window
        val env: StreamExecutionEnvironment =
            StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        val socketDS: DataStream[String] = env.socketTextStream("localhost", 9999)

        val mapDS: DataStream[(String, Int)] = socketDS.map((_,1))

        // 分流
        val socketKS: KeyedStream[(String, Int), String] = mapDS.keyBy(_._1)

        val aggregateDS = socketKS.timeWindow(Time.seconds(3)).aggregate(
            new MyAggregateFunction
        )

        aggregateDS.print("aggregate>>>")

        env.execute()
    }
    // 自定义累加器函数
    // 1. 继承AggregateFunction， 定义泛型：输入In, 累加器中间处理值Acc, 输出Out
    // 2. 重写方法
    class MyAggregateFunction extends AggregateFunction[(String, Int), Int, Int] {
        // 创建累加器
        override def createAccumulator(): Int = 0

        // 将输入值对累加器进行更新
        override def add(value: (String, Int), accumulator: Int): Int = {
            println("acc.....")
            accumulator + value._2
        }

        // 获取累加器的值
        override def getResult(accumulator: Int): Int = accumulator

        // 合并累加器的值
        override def merge(a: Int, b: Int): Int = a + b
    }

}
