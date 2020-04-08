package com.atguigu.bigdata.flink

import java.text.SimpleDateFormat

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object Flink41_API_Time_Watermark3 {

    def main(args: Array[String]): Unit = {

        val env: StreamExecutionEnvironment =
            StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        // 设置时间语义
        // 默认会每200ms来生成watermark
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        //val dataDS: DataStream[String] = env.socketTextStream("localhost",9999)
        // 如果数据源为file，那么在读取数据后进行计算时
        // 即使数据窗口没有提交，那么在文件读取结束后也会自动计算
        // 因为flink会在文件读完完毕后，将watermark设置为Long的最大值。
        // 需要将所有未计算的窗口全部进行计算
        //val dataDS: DataStream[String] = env.readTextFile("input/sensor-data.log")
        val dataDS: DataStream[String] = env.socketTextStream("localhost", 9999)
        val sensorDS: DataStream[WaterSensor] = dataDS.map(
            data => {
                val datas = data.split(",")
                WaterSensor(datas(0), datas(1).toLong, datas(2).toInt)
            }
        )
        val markDS: DataStream[WaterSensor] = sensorDS.assignTimestampsAndWatermarks(
            new BoundedOutOfOrdernessTimestampExtractor[WaterSensor](Time.seconds(3)) {
                // 抽取事件时间,以毫秒为单位
                override def extractTimestamp(element: WaterSensor): Long = {
                    element.ts * 1000L
                }
            }
        )
        val applyDS: DataStream[String] = markDS
                .keyBy(_.id)
                .timeWindow(Time.seconds(5))
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
