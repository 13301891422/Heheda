package com.atguigu.bigdata.flink

import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}

object Flink09_API_Source_File {

    def main(args: Array[String]): Unit = {

        // TODO Source
        val env: StreamExecutionEnvironment =
            StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        // 从数据源（Source）中获取数据
        // 2. 从文件中获取数据
        // HDFS文件
        //val lineDS: DataStream[String] = env.readTextFile("input/sensor.txt")
        // 在当前的环境中如果直接访问HDFS的文件，会发生错误，主要是依赖关系中没有hadoop
        // 可以直接引入jar包（flink-shaded-hadoop-2-uber-2.7.5-7.0.jar）就可以了
        val lineDS: DataStream[String] = env.readTextFile("hdfs://linux1:9000/test")

//        val waterSersorDS: DataStream[WaterSensor] = lineDS.map(
//            line => {
//                val datas: Array[String] = line.split(",")
//                WaterSensor(datas(0), datas(1).toLong, datas(2).toInt)
//            }
//        )

        lineDS.print("file>>>>")

        env.execute()
    }
}
