package com.atguigu.bigdata.flink

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.table.api.{Table, TableEnvironment}
import org.apache.flink.table.api.scala.{StreamTableEnvironment,_}

object Flink56_API_Table {

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

        // TODO 使用Table API

        // 获取TableAPI环境
        val tableEnv: StreamTableEnvironment = TableEnvironment.getTableEnvironment(env)
        // 将数据转换为一张表
        val table: Table = tableEnv.fromDataStream(sensorDS)

        val result: DataStream[String] = table.select("id").toAppendStream[String]

        result.print("table>>>")

        env.execute()
    }

}
