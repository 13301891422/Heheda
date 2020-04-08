package com.atguigu.bigdata.flink.application

import com.atguigu.bigdata.flink.common.TApplication
import com.atguigu.bigdata.flink.controller.LoginFailAnalysesController

object LoginFailAnalysesApplication extends App with TApplication{
    start {
        val controller = new LoginFailAnalysesController
        controller.execute()
    }

}
