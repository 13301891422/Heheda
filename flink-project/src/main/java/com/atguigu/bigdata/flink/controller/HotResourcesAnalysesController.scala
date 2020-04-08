package com.atguigu.bigdata.flink.controller

import com.atguigu.bigdata.flink.common.TController
import com.atguigu.bigdata.flink.service.HotResourcesAnalysesService

/**
  * 热门资源浏览量控制器
  */
class HotResourcesAnalysesController extends TController{
    private val hotResourcesAnalysesService = new HotResourcesAnalysesService
    override def execute(): Unit = {
        val result = hotResourcesAnalysesService.analyses()
        result.print
    }
}
