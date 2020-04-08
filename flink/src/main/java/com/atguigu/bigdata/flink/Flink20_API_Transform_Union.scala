package com.atguigu.bigdata.flink

import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}

object Flink20_API_Transform_Union {

    def main(args: Array[String]): Unit = {

        // 转换
        val env: StreamExecutionEnvironment =
            StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(2)

        // TODO Transform - union
        val ds1 = env.fromCollection(List(1,2))
        val ds2 = env.fromCollection(List("a","b"))

        // union 合并两个流，这两个流的类型必须一致
        val ds3: DataStream[String] = ds1.map(_.toString).union(ds2)

        ds3.print("union>>>")


        env.execute()
    }
}
