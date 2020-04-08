package com.atguigu.bigdata.flink

import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}

object Flink18_API_Transform_Connect {

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
        val cbds: DataStream[(String, Int)] =
            dataSS.select("c", "b")

//        ads.print("aaaaa>>>")
//        bds.print("bbbbb>>>")
//        cds.print("ccccc>>>")
//        cbds.print("cbcbcb>>>")
        //
        val abDS: ConnectedStreams[(String, Int), (String, Int)] = ads.connect(bds)

        // ConnectedStreams => DataStream
        val mapDS: DataStream[(String, Int)] = abDS.map(
            t => t,
            t => t
        )

        mapDS.print("map>>>>")

        env.execute()
    }
}
