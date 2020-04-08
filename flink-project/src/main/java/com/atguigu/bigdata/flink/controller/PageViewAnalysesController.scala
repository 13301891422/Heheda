package com.atguigu.bigdata.flink.controller

import com.atguigu.bigdata.flink.common.TController
import com.atguigu.bigdata.flink.service.PageViewAnalysesService

class PageViewAnalysesController extends TController{

    private val pageViewAnalysesService = new PageViewAnalysesService

    override def execute() = {
        val result = pageViewAnalysesService.analyses()
        result.print
    }
}
