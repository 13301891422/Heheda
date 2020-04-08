package com.atguigu.bigdata.flink.application

import com.atguigu.bigdata.flink.common.TApplication
import com.atguigu.bigdata.flink.controller.UniqueVisitorAnalysesController

object UniqueVisitorAnalysesApplication extends App with TApplication{

    start {
        val controller = new UniqueVisitorAnalysesController
        controller.execute()
    }
}
