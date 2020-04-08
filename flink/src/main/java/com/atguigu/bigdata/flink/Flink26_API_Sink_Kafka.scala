package com.atguigu.bigdata.flink

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011

object Flink26_API_Sink_Kafka {

    def main(args: Array[String]): Unit = {

        // 转换
        val env: StreamExecutionEnvironment =
            StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        // 对数据流的处理可以采用Scala API，也可以采用Java API
        val dataDS: DataStream[String] = env.fromCollection(
            List("a", "b", "c")
        )

        // TODO Sink - 将数据发送到Kafka
        val topic = "waterSensor1"
        val kafkaSink = dataDS.addSink(new FlinkKafkaProducer011[String](
            "linux1:9092",
            topic,
            new SimpleStringSchema()
        ))

        env.execute()
    }

}
