package com.atguigu.bigdata.flink

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

object Flink10_API_Source_Kafka {

    def main(args: Array[String]): Unit = {

        // TODO Source
        val env: StreamExecutionEnvironment =
            StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        // Flink默认没有提供Kafka的数据源采集功能
        // 所以如果想要采集kafka的数据，必须增加相关的依赖
        // 并且需要让flink可以支持kafka操作:增加数据源
        // 主题
        val topic = "waterSensor1"
        val properties = new java.util.Properties()
        properties.setProperty("bootstrap.servers", "linux1:9092")
        properties.setProperty("group.id", "consumer-group")
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
        properties.setProperty("auto.offset.reset", "latest")

        // 从kafka中获取数据
        val kafkaDS: DataStream[String] =
            env.addSource(
                new FlinkKafkaConsumer011[String](
                    topic,
                    new SimpleStringSchema(),
                    properties) )

        val waterSersorDS = kafkaDS.map(
            line => {
                val datas: Array[String] = line.split(",")
                WaterSensor( datas(0), datas(1).toLong, datas(2).toInt )
            }
        )

        waterSersorDS.print("kafka>>>>")

        env.execute()
    }
}
