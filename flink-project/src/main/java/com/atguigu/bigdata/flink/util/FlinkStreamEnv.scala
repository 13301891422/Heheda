package com.atguigu.bigdata.flink.util

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
  * Flink流环境
  */
object FlinkStreamEnv {
    private val envLocal = new ThreadLocal[StreamExecutionEnvironment]

    /**
      * 环境初始化
      */
    def init() = {
        val env: StreamExecutionEnvironment =
            StreamExecutionEnvironment.getExecutionEnvironment

        env.setParallelism(1)
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        envLocal.set(env)
//        env.setStateBackend()
//        env.getConfig.setAutoWatermarkInterval()

        env
    }

    /**
      * 获取环境
      */
    def get() = {
        var env = envLocal.get()
        if ( env == null ) {
            env = init()
        }
        env
    }

    def clear(): Unit = {
        envLocal.remove()
    }

    /**
      * 执行环境
      */
    def execute(): Unit = {
        get().execute("application")
    }
}
