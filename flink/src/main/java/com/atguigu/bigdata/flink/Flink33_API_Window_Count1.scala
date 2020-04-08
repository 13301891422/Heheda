package com.atguigu.bigdata.flink

import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow

object Flink33_API_Window_Count1 {

    def main(args: Array[String]): Unit = {

        // TODO  API - Window
        val env: StreamExecutionEnvironment =
            StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        val socketDS: DataStream[String] = env.socketTextStream("localhost", 9999)

        val mapDS: DataStream[(String, Int)] = socketDS.map((_,1))

        // 分流
        val socketKS: KeyedStream[(String, Int), String] = mapDS.keyBy(_._1)

        // TODO 计数窗口：根据数量来设定窗口的范围
        // 如果多个窗口之间重合，称之为滑动窗口
        val socketWS: WindowedStream[(String, Int), String, GlobalWindow] =
            socketKS.countWindow(3, 2)

        // 滑动窗口是根据滑动的幅度触发窗口的计算
        val reduceDS: DataStream[(String, Int)] = socketWS.reduce(
            (t1, t2) => {
                (t1._1, t1._2 + t2._2)
            }
        )
        reduceDS.print("count")

        env.execute()
    }
}
