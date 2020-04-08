package com.atguigu.bigdata.flink.controller

import com.atguigu.bigdata.flink.common.TController
import com.atguigu.bigdata.flink.service.AppMarketAnalysesService

class AppMarketAnalysesController extends TController{

    private val appMarketAnalysesService = new AppMarketAnalysesService

    override def execute() = {
        val result = appMarketAnalysesService.analyses()
        result.print
    }
}
