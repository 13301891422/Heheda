package com.atguigu.bigdata.flink

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

import scala.util.Random

object Flink11_API_Source_DIY {

    def main(args: Array[String]): Unit = {

        // TODO Source
        val env: StreamExecutionEnvironment =
            StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        // 创建自定义数据源
        val waterSensorDS: DataStream[WaterSensor] = env.addSource( new MySource() )

        waterSensorDS.print("sensor>>>")

        env.execute()
    }
    // 自定义数据源
    // 1. 实现（混入）SourceFunction接口（特质）
    // 2. 重写run, cancel方法。
    class MySource extends SourceFunction[WaterSensor] {

        private var runflg = true

        // 生成数据
        override def run(ctx: SourceFunction.SourceContext[WaterSensor]): Unit = {

            while ( runflg ) {

                // 生成水位传感器数据
                // 0001,110230232,2
                val data = WaterSensor( "0001", 110230232, new Random().nextInt(10) )
                // 将生成的数据放置到Flink环境中
                ctx.collect(data)
                Thread.sleep(200)
            }
        }

        // 取消生成
        override def cancel(): Unit = {
            runflg = false
        }
    }
}
