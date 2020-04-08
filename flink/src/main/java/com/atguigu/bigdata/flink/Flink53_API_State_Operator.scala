package com.atguigu.bigdata.flink

import org.apache.flink.api.common.state.ValueState
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.util.Collector

object Flink53_API_State_Operator {

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

        val wsDS = sensorDS.assignAscendingTimestamps(_.ts * 1000)

        val sensorKS: KeyedStream[WaterSensor, String] =
            wsDS.keyBy(_.id)

        val value: DataStream[WaterSensor] = sensorKS.mapWithState(
            (w, opt: Option[Int]) => {
                (w, Option(w.height + opt.getOrElse(0)))
            }
        )
        value

        sensorKS.print("process>>>>")

        env.execute()
    }

}
