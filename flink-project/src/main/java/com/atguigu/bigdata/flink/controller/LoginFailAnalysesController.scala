package com.atguigu.bigdata.flink.controller

import com.atguigu.bigdata.flink.common.TController
import com.atguigu.bigdata.flink.service.LoginFailAnalysesService

class LoginFailAnalysesController extends TController {
    private val loginFailAnalysesService = new LoginFailAnalysesService
    override def execute() = {
        val result = loginFailAnalysesService.analyses()
        result.print
    }
}
