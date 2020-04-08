package com.atguigu.bigdata.flink

import java.text.SimpleDateFormat

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object Flink40_API_Time_Watermark2 {

    def main(args: Array[String]): Unit = {

        // TODO  API - Watermark
        // Flink对迟到的数据处理进行了功能的实现
        // 1. Window ： 窗口
        // 2. Watermark ： 水位线
        // 3. allowedLateness ： 允许接收延迟数据
        // 4. sideOutputLateData ： 将晚到的数据（指定的窗口已经计算完）放置在侧输出流中

        //
        val env: StreamExecutionEnvironment =
            StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        // Watermark : 水位线（水印）
        val dataDS: DataStream[String] = env.socketTextStream("localhost",9999)
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

        // 如果指定的窗口已经计算完毕，不再接收新的数据，原则上来讲不再接收的数据就会丢弃
        // 如果必须要统计，课时窗口又不在接收，那么可以将数据放置在一个特殊的流中
        // 这个流称之为侧输出流:SideOutput

        val outputTag = new OutputTag[WaterSensor]("lateData")

        val applyDS: DataStream[String] = markDS
                .keyBy(_.id)
                .timeWindow(Time.seconds(5))
                .allowedLateness(Time.seconds(2))
                .sideOutputLateData(outputTag)
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
        // 将晚到的数据进行计算
        val lateDataDS: DataStream[WaterSensor] = applyDS.getSideOutput(outputTag)
        lateDataDS.print("side>>>")


        env.execute()
    }

}
