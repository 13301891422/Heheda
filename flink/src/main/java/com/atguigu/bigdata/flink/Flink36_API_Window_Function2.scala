package com.atguigu.bigdata.flink

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object Flink36_API_Window_Function2 {

    def main(args: Array[String]): Unit = {

        // TODO  API - Window
        val env: StreamExecutionEnvironment =
            StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        val socketDS: DataStream[String] = env.socketTextStream("localhost", 9999)

        val mapDS: DataStream[(String, Int)] = socketDS.map((_,1))

        // 分流
        val socketKS: KeyedStream[(String, Int), String] = mapDS.keyBy(_._1)

        val processDS = socketKS.timeWindow(Time.seconds(10))
            .process(new MyProcessWindow)

        processDS.print("process>>>")

        env.execute()
    }
    // 自定义全窗口函数：当窗口结束时，进行统一的计算
    // 1. 继承ProcessWindowFunction，定义泛型
    // 2. 重写process方法

    // 这个函数类处理数据会更加灵活。
    class MyProcessWindow extends ProcessWindowFunction[(String, Int),String, String, TimeWindow]{
        override def process(
                key: String, // 数据的key
                context: Context, // 上下文环境
                elements: Iterable[(String, Int)], //窗口中所有相同key的数据
                out: Collector[String] // 收集器 用于将数据输出到Sink
        ): Unit = {
            val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
            out.collect("窗口启动时间:" + sdf.format(new Date(context.window.getStart)))
            out.collect("窗口结束时间:" + sdf.format(new Date(context.window.getEnd)))
            out.collect("计算的数据为 ：" + elements.toList)
            out.collect("***********************************")
        }
    }

}
