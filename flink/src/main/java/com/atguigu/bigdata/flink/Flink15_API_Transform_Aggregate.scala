package com.atguigu.bigdata.flink

import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}

object Flink15_API_Transform_Aggregate {

    def main(args: Array[String]): Unit = {

        // 转换
        val env: StreamExecutionEnvironment =
            StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(2)

        // TODO Transform - keyBy
        val listDS: DataStream[(String, Int)] = env.fromCollection(
            List(
                ("a",1),("b",1),("a",2),("b",2)
            )
        )

        // Scala
        val dataKS: KeyedStream[(String, Int), String] = listDS.keyBy(_._1)

        //val sumDS: DataStream[(String, Int)] = dataKS.sum(1)
        //val minDS: DataStream[(String, Int)] = dataKS.min(1)
        val maxDS: DataStream[(String, Int)] = dataKS.max(1)

        maxDS.print("sum>>>")

        env.execute()
    }
}
