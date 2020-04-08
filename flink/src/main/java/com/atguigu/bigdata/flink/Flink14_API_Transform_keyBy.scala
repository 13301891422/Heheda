package com.atguigu.bigdata.flink

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}

object Flink14_API_Transform_keyBy {

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
        // Java
        //val dataKS: KeyedStream[(String, Int), Tuple] = listDS.keyBy(0)

        //dataKS.print("key>>>")

        env.execute()
    }
}
