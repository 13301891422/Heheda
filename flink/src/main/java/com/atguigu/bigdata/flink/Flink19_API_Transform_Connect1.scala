package com.atguigu.bigdata.flink

import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}

object Flink19_API_Transform_Connect1 {

    def main(args: Array[String]): Unit = {

        // 转换
        val env: StreamExecutionEnvironment =
            StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(2)

        // TODO Transform - connect
        val ds1 = env.fromCollection(List(1,2))
        val ds2 = env.fromCollection(List("a","b"))

        val cs: ConnectedStreams[Int, String] = ds1.connect(ds2)

        val ds3: DataStream[Any] = cs.map(
            num => num,
            s => s
        )
        ds3.print("ds>>>")

        env.execute()
    }
}
