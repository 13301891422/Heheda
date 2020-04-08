package com.atguigu.bigdata.flink

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.client.Requests

object Flink29_API_Sink_MySQL {

    def main(args: Array[String]): Unit = {

        // 转换
        val env: StreamExecutionEnvironment =
            StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        // 对数据流的处理可以采用Scala API，也可以采用Java API
        val dataDS: DataStream[String] = env.fromCollection(
            List("a", "b", "c")
        )

        // TODO 将数据保存到Mysql中
        dataDS.addSink( new MySQLSink() )

        env.execute()
    }
    // 自定义Sink
    // 1. 继承RichSinkFunction，定义泛型
    // 2. 重写方法
    class MySQLSink extends RichSinkFunction[String]{

        var conn:Connection = null
        var pstat : PreparedStatement = null

        // 建立链接
        override def open(parameters: Configuration): Unit = {
            //Class.forName("jdbc:mysql://hadoop01:3306/rdd")
            conn = DriverManager
                .getConnection("jdbc:mysql://linux1:3306/rdd"
                            ,"root","000000"
                )
            pstat = conn.prepareStatement(
                "INSERT INTO user (id, name, age) VALUES (?, ?, ?)"
            )
        }

        override def invoke(value: String, context: SinkFunction.Context[_]): Unit = {
            pstat.setInt(1, 1)
            pstat.setString(2, value)
            pstat.setInt(3, 10)
            pstat.execute()
        }

        // 释放链接
        override def close(): Unit = {
            if ( pstat != null ) {
                pstat.close()
            }
            if ( conn != null ) {
                conn.close()
            }

        }
    }

}
