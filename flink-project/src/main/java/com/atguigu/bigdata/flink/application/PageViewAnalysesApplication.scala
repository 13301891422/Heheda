package com.atguigu.bigdata.flink.application

import com.atguigu.bigdata.flink.common.TApplication
import com.atguigu.bigdata.flink.controller.PageViewAnalysesController

/**
  * 页面访问量统计分析
  */
object PageViewAnalysesApplication extends App with TApplication{

    start {
        val controller = new PageViewAnalysesController
        controller.execute()
    }
}
