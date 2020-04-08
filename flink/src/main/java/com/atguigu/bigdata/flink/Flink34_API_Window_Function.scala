package com.atguigu.bigdata.flink

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow

object Flink34_API_Window_Function {

    def main(args: Array[String]): Unit = {

        // TODO  API - Window
        val env: StreamExecutionEnvironment =
            StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        val socketDS: DataStream[String] = env.socketTextStream("localhost", 9999)

        val mapDS: DataStream[(String, Int)] = socketDS.map((_,1))

        // 分流
        val socketKS: KeyedStream[(String, Int), String] = mapDS.keyBy(_._1)

        val reduceDS = socketKS.timeWindow(Time.seconds(3)).reduce(
            new MyReduceFunction
        )

        reduceDS.print("reduce>>>")

        env.execute()
    }
    // 自定义增量聚合窗口函数
    // 1. 继承ReduceFunction，定义泛型
    // 2. 重写方法

    // reduce方法第一条数据不做处理，从第二条开始，每来一条数据，就会计算一次
    // 当窗口结束时，直接返回结算结果。
    class MyReduceFunction extends ReduceFunction[(String, Int)] {
        override def reduce(t1: (String, Int), t2: (String, Int)): (String, Int) = {
            println("reduce = " + t1._2 + "+"+ t2._2)
            ( t1._1, t1._2 + t2._2 )
        }
    }
}
