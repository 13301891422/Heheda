package com.atguigu.bigdata.flink

import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.util.Collector

object Flink51_API_Process_CoProcess {

    def main(args: Array[String]): Unit = {

        // 转换
        val env: StreamExecutionEnvironment =
            StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(2)

        // TODO Transform - connect
        val listDS: DataStream[(String, Int)] = env.fromCollection(
            List(
                ("abc",1),("bbc",2),("acb",3),("bcb",4),("aba",5),("aab",6)
            )
        )

        // 将数据根据条件进行分流
        // 给每个流起一个名字，方便后续访问
        // 此方法不推荐使用，可以采用侧输出流代替
        val dataSS: SplitStream[(String, Int)] = listDS.split(
            t => {
                val key = t._1
                val splitKey = key.substring(1, 2)
                if (splitKey == "a") {
                    Seq("a", "aa")
                } else if (splitKey == "b") {
                    Seq("b", "bb")
                } else {
                    Seq("c", "cc")
                }
            }
        )

        val ads: DataStream[(String, Int)] = dataSS.select("a")
        val bds: DataStream[(String, Int)] = dataSS.select("b")
        val cds: DataStream[(String, Int)] = dataSS.select("c")
        //
        val abDS: ConnectedStreams[(String, Int), (String, Int)] = ads.connect(bds)

        val coDS: DataStream[String] = abDS.process(new CoProcessFunction[(String, Int), (String, Int), String] {
            override def processElement1(value: (String, Int), ctx: CoProcessFunction[(String, Int), (String, Int), String]#Context, out: Collector[String]): Unit = {
                out.collect("key1 = " + value._1)
            }

            override def processElement2(value: (String, Int), ctx: CoProcessFunction[(String, Int), (String, Int), String]#Context, out: Collector[String]): Unit = {
                out.collect("key2 = " + value._1)
            }
        })

        coDS.print("map>>>>")

        env.execute()
    }
}
