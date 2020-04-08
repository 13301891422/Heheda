package com.atguigu.bigdata.flink.application

import com.atguigu.bigdata.flink.common.TApplication
import com.atguigu.bigdata.flink.controller.HotResourcesAnalysesController

/**
  * 热门资源浏览量排行
  */
object HotResourcesAnalysesApplication extends App with TApplication{

    start {
        val controller = new HotResourcesAnalysesController
        controller.execute()
    }
}
