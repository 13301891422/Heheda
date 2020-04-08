package com.atguigu.bigdata.flink

import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}


object Flink59_API_CEP {

    def main(args: Array[String]): Unit = {

        val env: StreamExecutionEnvironment =
            StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        val dataDS: DataStream[String] = env.readTextFile("input/sensor-data.log")

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
                // 1.1 开始准备定义规则
                // 规则必须以begin开始，表示开始定义规则
                .begin[WaterSensor]("begin")
                // 1.2 增加规则（条件）
                //.where(_.height < 4)
                // 增加新的条件，多个条件应该同时满足
                //.where(_.height > 2)
                // 增加新的条件，多个条件满足任意一个都可以
                //.or(_.height > 2)


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
