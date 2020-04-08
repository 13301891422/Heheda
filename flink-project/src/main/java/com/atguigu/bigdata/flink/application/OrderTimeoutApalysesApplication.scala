package com.atguigu.bigdata.flink.application

import com.atguigu.bigdata.flink.common.TApplication
import com.atguigu.bigdata.flink.controller.OrderTimeoutApalysesController

object OrderTimeoutApalysesApplication extends App with TApplication{

    start {
        val controller = new OrderTimeoutApalysesController
        controller.execute()
    }
}
