package com.atguigu.bigdata.flink.application

import com.atguigu.bigdata.flink.common.TApplication
import com.atguigu.bigdata.flink.controller.AppMarketAnalysesController

object AppMarketAnalysesApplication extends App with TApplication {

    start {
        val controller = new AppMarketAnalysesController
        controller.execute()
    }
}
