package com.atguigu.bigdata.flink

import java.text.SimpleDateFormat

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.{AssignerWithPeriodicWatermarks, AssignerWithPunctuatedWatermarks}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object Flink43_API_Time_Watermark5 {

    def main(args: Array[String]): Unit = {

        val env: StreamExecutionEnvironment =
            StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)


        val dataDS: DataStream[String] = env.socketTextStream("localhost", 9999)
        val sensorDS: DataStream[WaterSensor] = dataDS.map(
            data => {
                val datas = data.split(",")
                WaterSensor(datas(0), datas(1).toLong, datas(2).toInt)
            }
        )
        val markDS: DataStream[WaterSensor] =
            sensorDS.assignTimestampsAndWatermarks(
                // 自定义事件时间的抽取和生成水位线watermark
                new AssignerWithPunctuatedWatermarks[WaterSensor] {
                    override def checkAndGetNextWatermark(lastElement: WaterSensor, extractedTimestamp: Long): Watermark =  {
                        println("checkAndGetNextWatermark.....")
                        // 间歇性生成水位线数据
                        new Watermark(extractedTimestamp)
                    }

                    override def extractTimestamp(element: WaterSensor, previousElementTimestamp: Long): Long = {
                        // 抽取事件时间
                        element.ts * 1000
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
