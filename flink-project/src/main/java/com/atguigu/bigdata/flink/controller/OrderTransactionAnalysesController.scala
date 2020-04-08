package com.atguigu.bigdata.flink.controller

import com.atguigu.bigdata.flink.common.TController
import com.atguigu.bigdata.flink.service.OrderTransactionAnalysesService

class OrderTransactionAnalysesController extends TController{

    private val orderTransactionAnalysesService = new OrderTransactionAnalysesService

    override def execute(): Unit = {
        val result = orderTransactionAnalysesService.analyses()
        result.print
    }
}
