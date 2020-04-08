package com.atguigu.bigdata.flink

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object Flink37_API_Time_Event {

    def main(args: Array[String]): Unit = {

        // TODO  API - Window
        val env: StreamExecutionEnvironment =
            StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        // 默认情况下，flink处理数据采用的时间为Processing Time
        // 但是实际业务中一般都采用EventTime
        // 可以通过环境对象设置时间语义
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)


        env.execute()
    }

}
