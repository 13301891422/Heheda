package com.atguigu.bigdata.flink

import org.apache.flink.api.common.functions.{MapFunction, RichMapFunction, RuntimeContext}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}

object Flink24_API_Transform_RichFunction {

    def main(args: Array[String]): Unit = {

        // 转换
        val env: StreamExecutionEnvironment =
            StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        // 对数据流的处理可以采用Scala API，也可以采用Java API
        val dataDS: DataStream[Int] = env.fromCollection(
            List(1, 2, 3, 4)
        )

        val mapDS: DataStream[String] = dataDS.map( new MyMapRichFunction )

        mapDS.print("map")

        env.execute()
    }
    // 自定义富函数
    // 1. 继承RichMapFunction, 定义泛型
    // 2. 重写方法
    // 3. 富函数提供了更加强大的方法，包括和环境相关的功能
    class MyMapRichFunction extends RichMapFunction[Int, String] {

        //override def getRuntimeContext: RuntimeContext = super.getRuntimeContext

        override def open(parameters: Configuration): Unit = {

        }

        override def map(value: Int): String = {
            getRuntimeContext.getTaskName + "=" + value
        }

        override def close(): Unit = {

        }
    }

}
