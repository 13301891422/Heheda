package com.atguigu.bigdata.flink.application

import com.atguigu.bigdata.flink.common.TApplication
import com.atguigu.bigdata.flink.controller.AdvertClickAnalysesController

object AdvertClickAnalysesApplication extends App with TApplication {

    start {
        val controller = new AdvertClickAnalysesController
        controller.execute()
    }
}
