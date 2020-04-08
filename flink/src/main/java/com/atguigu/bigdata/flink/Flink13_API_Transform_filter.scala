package com.atguigu.bigdata.flink

import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}

object Flink13_API_Transform_filter {

    def main(args: Array[String]): Unit = {

        // 转换
        val env: StreamExecutionEnvironment =
            StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)


        // TODO Transform - filter
        val listDS: DataStream[Int] = env.fromCollection(
            List(
                1,2,3,4
            )
        )

        // 根据过滤规则对数据进行筛选过滤
        val filterDS: DataStream[Int] = listDS.filter(_%2 == 0)

        filterDS.print("filter>>>")

        env.execute()
    }
}
