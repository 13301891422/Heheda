package com.atguigu.bigdata.flink.controller

import com.atguigu.bigdata.flink.common.TController
import com.atguigu.bigdata.flink.service.AdvertClickAnalysesService

class AdvertClickAnalysesController extends TController {

    private val advertClickAnalysesService = new AdvertClickAnalysesService

    override def execute() = {
        val result = advertClickAnalysesService.analyses()
        result.print
    }
}
