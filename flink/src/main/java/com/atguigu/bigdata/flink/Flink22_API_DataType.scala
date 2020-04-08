package com.atguigu.bigdata.flink

import com.atguigu.bigdata.flink.bean.Person
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}

object Flink22_API_DataType {

    def main(args: Array[String]): Unit = {

        // 转换
        val env: StreamExecutionEnvironment =
            StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        // 使用样例类获取DataStream
//        val wsDS: DataStream[WaterSensor] = env.fromCollection(
//            List(1, 2, 3, 4)
//        ).map(num => WaterSensor("xxx", 1111L, num))

        // 使用Java对象获取DataStream
//        val personDS: DataStream[Person] = env.fromElements(
//            new Person(), new Person()
//        )
//        personDS.print("person>>>")

        val personDS: DataStream[Person] = env.fromCollection(
            List(new Person, new Person)
        )
        personDS.print("person>>>")



        env.execute()
    }
}
