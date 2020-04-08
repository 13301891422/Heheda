package com.atguigu.bigdata.flink


import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.typeinfo.{IntegerTypeInfo, TypeInformation}
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}


object Flink23_API_Transform_Function {

    def main(args: Array[String]): Unit = {

        // 转换
        val env: StreamExecutionEnvironment =
            StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        // 对数据流的处理可以采用Scala API，也可以采用Java API
        val dataDS: DataStream[Int] = env.fromCollection(
            List(1, 2, 3, 4)
        )

        //val mapDS: DataStream[Int] = dataDS.map(_*2)
        // 使用函数类完成数据结构的转换
        //val functionDS: DataStream[Int] = dataDS.map( new MyMapFunction )
        val functionDS1: DataStream[(Int, Int)] = dataDS.map( new MyMapFunction1 )

        dataDS.map(
            new MapFunction[Int, Int] {
                override def map(value: Int): Int = {
                    value * 2
                }
            }
        )

        functionDS1.print("map>>>")


        env.execute()
    }
    // 声明函数类
    // 1. 继承MapFunction， 确定泛型
    // 2. 重写方法
    class MyMapFunction extends MapFunction[Int, Int] {
        override def map(value: Int): Int = {
            value * 2
        }
    }
    class MyMapFunction1 extends MapFunction[Int, (Int, Int)] {
        override def map(value: Int): (Int, Int) = {
            (value, value)
        }
    }
}
