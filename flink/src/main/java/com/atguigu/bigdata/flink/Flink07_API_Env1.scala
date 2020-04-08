package com.atguigu.bigdata.flink

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object Flink07_API_Env1 {

    def main(args: Array[String]): Unit = {

        // 自动装配（加载功能）
        // SpringBoot

        // TODO 上下文环境
        // 直接获取执行环境对象时，可以动态识别环境的信息，判断是否创建本地环境还是集群环境
        //ExecutionEnvironment.getExecutionEnvironment
        // 创建本地环境对象
        //ExecutionEnvironment.createLocalEnvironment(1)
        // 创建远程环境（集群）对象
        //ExecutionEnvironment.createRemoteEnvironment()


    }
}
