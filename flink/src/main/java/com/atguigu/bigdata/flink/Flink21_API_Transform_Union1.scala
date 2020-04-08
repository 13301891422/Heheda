package com.atguigu.bigdata.flink

import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}

object Flink21_API_Transform_Union1 {

    def main(args: Array[String]): Unit = {

        // 转换
        val env: StreamExecutionEnvironment =
            StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(2)

        // TODO Transform - union
        val ds1 = env.fromCollection(List(1,2))
        val ds2 = env.fromCollection(List("a","b"))
        val ds3 = env.fromCollection(List("c","d"))

        // union 合并两个流，这两个流的类型必须一致
        // 可以多个流同时合并
        val ds4: DataStream[String] = ds1.map(_.toString).union(ds2, ds3)

        ds4.print("union>>>")


        env.execute()
    }
}
