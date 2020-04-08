package com.atguigu.bigdata.flink.application

import com.atguigu.bigdata.flink.common.TApplication
import com.atguigu.bigdata.flink.controller.OrderTransactionAnalysesController

object OrderTransactionAnalysesApplication extends App with TApplication {

    start {
        val controller = new OrderTransactionAnalysesController
        controller.execute()
    }
}
