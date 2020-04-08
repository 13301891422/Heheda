package com.atguigu.bigdata.flink.common

import com.atguigu.bigdata.flink.util.FlinkStreamEnv
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

/**
  * 通用数据访问特质
  */
trait TDao {

    /**
      * 读取文件
      */
    def readTextFile( implicit path:String ) = {
        FlinkStreamEnv.get().readTextFile(path)
    }

    /**
      * 读取Kafka数据
      */
    def readKafka() = {
        val topic = "waterSensor1"
        val properties = new java.util.Properties()
        properties.setProperty("bootstrap.servers", "linux1:9092")
        properties.setProperty("group.id", "consumer-group")
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
        properties.setProperty("auto.offset.reset", "latest")

        // 从kafka中获取数据
        val kafkaDS: DataStream[String] =
            FlinkStreamEnv.get().addSource(
                new FlinkKafkaConsumer011[String](
                    topic,
                    new SimpleStringSchema(),
                    properties) )
        kafkaDS
    }

    /**
      * 读取Socket网络数据
      */
    def readSocket() = {
        FlinkStreamEnv.get().socketTextStream("localhost", 9999)
    }
}
