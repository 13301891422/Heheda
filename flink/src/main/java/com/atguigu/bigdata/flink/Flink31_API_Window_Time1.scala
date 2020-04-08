package com.atguigu.bigdata.flink

import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

object Flink31_API_Window_Time1 {

    def main(args: Array[String]): Unit = {

        // TODO  API - Window
        val env: StreamExecutionEnvironment =
            StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        val socketDS: DataStream[String] = env.socketTextStream("localhost", 9999)

        val mapDS: DataStream[(String, Int)] = socketDS.map((_,1))

        // 分流
        val socketKS: KeyedStream[(String, Int), String] = mapDS.keyBy(_._1)

        // TODO 时间窗口 : 以时间作为数据的处理范围
        // 如果多个窗口之间有重合，称之为滑动窗口
        // 可以设定滑动的幅度
        val socketWS: WindowedStream[(String, Int), String, TimeWindow] =
            socketKS.timeWindow(
                Time.seconds(3),
                Time.seconds(2)
            )

        val reduceDS: DataStream[(String, Int)] = socketWS.reduce(
            (t1, t2) => {
                (t1._1, t1._2 + t2._2)
            }
        )
        reduceDS.print("window>>>>")

        env.execute()
    }
}
