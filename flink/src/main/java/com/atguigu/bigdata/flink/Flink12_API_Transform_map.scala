package com.atguigu.bigdata.flink

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}

import scala.util.Random

object Flink12_API_Transform_map {

    def main(args: Array[String]): Unit = {

        // 转换
        // TODO Transform - map
        val env: StreamExecutionEnvironment =
            StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        //val numDS: DataStream[Int] = env.fromCollection(List(1,2,3,4))
        //val mapDS = numDS.map( _ * 2 )

        // TODO Transform - flatMap
        val listDS: DataStream[Int] = env.fromCollection(
            List(
                1,2,3,4
            )
        )
        //val flatDS: DataStream[Int] = listDS.flatMap(list=>list)
        //val flatDS: DataStream[Int] = listDS.flatMap(_.split(","))
        val flatDS: DataStream[Int] = listDS.flatMap(num=>List(num))

        flatDS.print("flatMap>>>")

        env.execute()
    }
}
