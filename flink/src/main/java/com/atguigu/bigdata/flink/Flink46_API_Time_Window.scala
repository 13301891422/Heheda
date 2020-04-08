package com.atguigu.bigdata.flink

import java.text.SimpleDateFormat

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.assigners.{EventTimeSessionWindows, SlidingEventTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object Flink46_API_Time_Window {

    def main(args: Array[String]): Unit = {

        val env: StreamExecutionEnvironment =
            StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
        // 改变水位线数据生成的周期
        //env.getConfig.setAutoWatermarkInterval(5000)

        val dataDS: DataStream[String] = env.socketTextStream("localhost", 9999)
        val sensorDS: DataStream[WaterSensor] = dataDS.map(
            data => {
                val datas = data.split(",")
                WaterSensor(datas(0), datas(1).toLong, datas(2).toInt)
            }
        )
        // 如果数据可以预期为有序的，那么可以简单使用方法设定事件时间
        val markDS: DataStream[WaterSensor] =
            sensorDS.assignAscendingTimestamps( _.ts * 1000 )

        val applyDS: DataStream[String] = markDS
                .keyBy(_.id)
                //.timeWindow(Time.seconds(5))
                // 设定EventTime的滚动窗口
                //.window(TumblingEventTimeWindows.of(Time.seconds(5)))
                // 设定EventTime的滑动窗口
                //.timeWindow(Time.seconds(5), Time.seconds(5))
                //.window(SlidingEventTimeWindows.of(Time.seconds(5), Time.seconds(5)))
                // 设定EventTime的会话窗口
                // 时间间隔应该大于session间隔数据，会触发窗口的计算
                // session窗口的划分以当前数据为开始，加5s为结束

                // 24 - 25 26- 29 - 35

                .window(EventTimeSessionWindows.withGap(Time.seconds(5)))
                .apply(
                    (key: String, window: TimeWindow, datas: Iterable[WaterSensor], out: Collector[String]) => {
                        val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
                        val start = window.getStart
                        val end = window.getEnd
                        out.collect(s"[${start}-${end}), 数据[${datas}]")
                    }
                )


        markDS.print("mark>>>")
        // 计算正常数据的窗口
        applyDS.print("window>>>")


        env.execute()
    }

}
