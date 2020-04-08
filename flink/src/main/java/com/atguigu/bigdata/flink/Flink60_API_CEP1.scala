package com.atguigu.bigdata.flink

import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}


object Flink60_API_CEP1 {

    def main(args: Array[String]): Unit = {

        val env: StreamExecutionEnvironment =
            StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        val dataDS: DataStream[String] = env.readTextFile("input/sensor.txt")

        val sensorDS: DataStream[WaterSensor] = dataDS.map(
            data => {
                val datas = data.split(",")
                WaterSensor(datas(0), datas(1).toLong, datas(2).toInt)
            }
        )

        // TODO 使用CEP处理复杂事件
        // TODO CEP需要增加相应的Scala版本依赖关系
        // TODO CEP的API分Java，Scala两种，这里推荐使用Scala

        // 1. 创建规则
        val pattern =
            Pattern
                .begin[WaterSensor]("begin")
                .where(_.id == "sensor_1")
                // 符合条件的次数
                //.times(2)
                // 近邻
                // 严格近邻：条件之间不能存在不符合条件的数据
                //.next("next")
                // 宽松近邻：条件之间可以存在不符合条件的数据
                // 前置条件只能出现一次
                //.followedBy("followed")
                // 非确定性宽松近邻：条件之间可以存在不符合条件的数据
                // 前置条件可以出现多次
                .followedByAny("followed")
                .where(_.id == "sensor_1")


        // 2. 应用规则：将规则应用到数据流中
        val sensorPS: PatternStream[WaterSensor] = CEP.pattern(sensorDS, pattern)

        // 3. 获取复合规则的结果
        val ds: DataStream[String] = sensorPS.select(
            map => {
                map.toString
            }
        )
        ds.print("cep>>>>")

        env.execute()
    }

}
