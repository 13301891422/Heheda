package com.atguigu.bigdata.flink

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

object Flink27_API_Sink_Redis {

    def main(args: Array[String]): Unit = {

        // 转换
        val env: StreamExecutionEnvironment =
            StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        // 对数据流的处理可以采用Scala API，也可以采用Java API
        val dataDS: DataStream[String] = env.fromCollection(
            List("a", "b", "c")
        )

        // TODO Sink - 将数据发送到Redis
        val conf = new FlinkJedisPoolConfig.Builder().setHost("linux4").setPort(6379).build()
        dataDS.addSink(
            new RedisSink[String](
                conf, new RedisMapper[String] {
                    override def getCommandDescription: RedisCommandDescription = {
                        new RedisCommandDescription(
                            RedisCommand.HSET,
                            "sensor")
                    }

                    override def getKeyFromData(t: String): String = {
                        t
                    }

                    override def getValueFromData(t: String): String = {
                        t
                    }
                }
            )
        )

        env.execute()
    }

}
