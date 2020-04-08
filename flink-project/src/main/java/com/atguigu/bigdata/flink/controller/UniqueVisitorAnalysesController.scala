package com.atguigu.bigdata.flink.controller

import com.atguigu.bigdata.flink.common.TController
import com.atguigu.bigdata.flink.service.UniqueVisitorAnalysesService

class UniqueVisitorAnalysesController extends TController{

    private val uniqueVisitorAnalysesService = new UniqueVisitorAnalysesService

    override def execute() = {
        val result = uniqueVisitorAnalysesService.analyses()
        result.print
    }
}
