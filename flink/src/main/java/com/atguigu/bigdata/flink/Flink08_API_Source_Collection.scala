package com.atguigu.bigdata.flink

import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}

object Flink08_API_Source1 {

    def main(args: Array[String]): Unit = {

        // TODO Source
        val env: StreamExecutionEnvironment =
            StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        // 从数据源（Source）中获取数据
        // 1. 从集合（内存）中获取数据
        val dataDS: DataStream[(String, Long, Int)] = env.fromCollection(
            List(
                ("0001", 110230232L, 2),
                ("0002", 110230232L, 3),
                ("0003", 110230232L, 4)
            )
        )

        val waterSersorDS: DataStream[WaterSensor] = dataDS.map(
            t => {
                WaterSensor(t._1, t._2, t._3)
            }
        )


        waterSersorDS.print("collect>>>>")

        env.execute()
    }
}
// 水位传感器
case class WaterSensor( id:String, ts:Long, height:Int )
