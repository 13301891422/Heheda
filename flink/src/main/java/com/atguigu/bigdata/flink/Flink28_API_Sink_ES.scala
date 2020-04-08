package com.atguigu.bigdata.flink

import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}
import org.apache.http.HttpHost
import org.elasticsearch.client.Requests

object Flink28_API_Sink_ES {

    def main(args: Array[String]): Unit = {

        // 转换
        val env: StreamExecutionEnvironment =
            StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        // 对数据流的处理可以采用Scala API，也可以采用Java API
        val dataDS: DataStream[String] = env.fromCollection(
            List("a", "b", "c")
        )

        val httpHosts = new java.util.ArrayList[HttpHost]()
        httpHosts.add(new HttpHost("linux1", 9200))

        val ds: DataStream[WaterSensor] = dataDS.map(
            s => {
                WaterSensor(s, 1L, 1)
            }
        )

        val esSinkBuilder = new ElasticsearchSink.Builder[WaterSensor]( httpHosts, new ElasticsearchSinkFunction[WaterSensor] {
            override def process(t: WaterSensor, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
                println("saving data: " + t)
                val json = new java.util.HashMap[String, String]()
                json.put("data", t.toString)
                val indexRequest = Requests.indexRequest().index("ws").`type`("readingData").source(json)
                requestIndexer.add(indexRequest)
                println("saved successfully")
            }
        } )

        // TODO 将数据发送到ES中
        ds.addSink(esSinkBuilder.build())

        env.execute()
    }

}
