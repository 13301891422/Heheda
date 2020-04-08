package com.atguigu.bigdata.flink

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.{AssignerWithPunctuatedWatermarks, KeyedProcessFunction}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.util.Collector

object Flink50_API_Req3 {

    def main(args: Array[String]): Unit = {

        val env: StreamExecutionEnvironment =
            StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        //val dataDS: DataStream[String] = env.socketTextStream("localhost", 9999)
        // 当flink读取到文件末尾的时候，会将watermark设定为Long最大值。
        val dataDS: DataStream[String] = env.readTextFile("input/sensor-data.log")

        val sensorDS: DataStream[WaterSensor] = dataDS.map(
            data => {
                val datas = data.split(",")
                WaterSensor(datas(0), datas(1).toLong, datas(2).toInt)
            }
        )


        val sensorKS: KeyedStream[WaterSensor, String] =
            sensorDS.keyBy(_.id)



        // TODO 监控水位传感器的水位值
        //      如果水位值在五秒钟之内连续上升，则报警。
        val processDS: DataStream[String] = sensorKS.process(
            new KeyedProcessFunction[String, WaterSensor, String] {
                val outputTag = new OutputTag[Int]("height")
                override def processElement(
                   value: WaterSensor, // 输入数据
                   ctx: KeyedProcessFunction[String, WaterSensor, String]#Context, // 上下文环境
                   out: Collector[String]): Unit = { // 输出
                    // 需求：将水位值高于5cm的值输出到side output

                    if ( value.height > 5 ) {
                        // 异常流
                        ctx.output(outputTag, value.height)
                    } else {
                        out.collect("水位高度 = " + value.height)
                    }
                }
            }
        )
        val outputTag = new OutputTag[Int]("height")
        processDS.print("process>>>>")
        val sideOutputDS: DataStream[Int] = processDS.getSideOutput(outputTag)
        sideOutputDS.print("side>>>")

        env.execute()
    }

}
