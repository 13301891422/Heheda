package com.atguigu.bigdata.flink.controller

import com.atguigu.bigdata.flink.common.TController
import com.atguigu.bigdata.flink.service.OrderTimeoutApalysesService

class OrderTimeoutApalysesController extends TController{

    private val orderTimeoutApalysesService = new OrderTimeoutApalysesService

    override def execute() = {

        val result = orderTimeoutApalysesService.analyses()
        result.print
    }
}
