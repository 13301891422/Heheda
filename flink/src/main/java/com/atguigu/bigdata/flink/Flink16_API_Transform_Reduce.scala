package com.atguigu.bigdata.flink

import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}

object Flink16_API_Transform_Reduce {

    def main(args: Array[String]): Unit = {

        // 转换
        val env: StreamExecutionEnvironment =
            StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(2)

        // TODO Transform - reduce
        val listDS: DataStream[(String, Int)] = env.fromCollection(
            List(
                ("a",1),("b",1),("a",2),("b",2),("a",3),("b",3)
            )
        )

        // Scala
        val dataKS: KeyedStream[(String, Int), String] = listDS.keyBy(_._1)

        val reduceDS: DataStream[(String, Int)] = dataKS.reduce(
            (t1, t2) => {
                (t1._1 + t2._1, t2._2 + t1._2)
            }
        )
        reduceDS.print("reduce>>>")

        env.execute()
    }
}
