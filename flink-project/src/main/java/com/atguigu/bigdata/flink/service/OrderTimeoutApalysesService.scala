package com.atguigu.bigdata.flink.service

import com.atguigu.bigdata.flink.bean.OrderEvent
import com.atguigu.bigdata.flink.common.{TDao, TService}
import com.atguigu.bigdata.flink.dao.OrderTimeoutApalysesDao
import com.atguigu.bigdata.flink.function.OrderTimeoutKeyedProcessFunction
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

class OrderTimeoutApalysesService extends TService {

    private val orderTimeoutApalysesDao = new OrderTimeoutApalysesDao

    override def getDao(): TDao = orderTimeoutApalysesDao

    def analysesNormal(): Unit = {

        // TODO 获取订单交易数据
        val dataDS: DataStream[String] = orderTimeoutApalysesDao.readTextFile("input/OrderLog.csv")
        val orderDS = dataDS.map(
            data => {
                val datas = data.split(",")
                OrderEvent(datas(0).toLong,datas(1),datas(2),datas(3).toLong)
            }
        )

        val orderTimeDS = orderDS.assignAscendingTimestamps(_.eventTime * 1000L)

        val orderKS: KeyedStream[OrderEvent, Long] = orderTimeDS.keyBy(_.orderId)

        // 定义规则
        val pattern =
            Pattern
                    .begin[OrderEvent]("begin")
                    .where(_.eventType == "create")
                    .followedBy("followed")
                    .where(_.eventType == "pay")
                    .within(Time.minutes(15))




        // 应用规则
        val orderPS: PatternStream[OrderEvent] = CEP.pattern(orderKS, pattern)

        // 获取结果
        orderPS.select(
            //
            map => {
                val order = map("begin").iterator.next()
                val pay = map("followed").iterator.next()
                var s = "订单ID :" + order.orderId
                s += "共耗时 " +(pay.eventTime - order.eventTime)+ " 秒"
                s
            }
        )
    }

    override def analyses() = {
        //analysesNormal

        // Timeout
        //analysesTimeout

        analysesTimeoutNoCEP
    }

    def analysesTimeoutNoCEP() = {

        // TODO 不使用CEP方式完成订单支付超时的功能
        val dataDS: DataStream[String] = orderTimeoutApalysesDao.readTextFile("input/OrderLog.csv")
        //val dataDS: DataStream[String] = orderTimeoutApalysesDao.readSocket()
        val orderDS = dataDS.map(
            data => {
                val datas = data.split(",")
                OrderEvent(datas(0).toLong,datas(1),datas(2),datas(3).toLong)
            }
        )

        val orderTimeDS = orderDS.assignAscendingTimestamps(_.eventTime * 1000L)

        val orderKS: KeyedStream[OrderEvent, Long] = orderTimeDS.keyBy(_.orderId)

        // TODO 一条数据来了就做处理
        // 因为使用keyBy，所以相同的订单数据会到一个流中
        val orderProcessDS = orderKS.process(new OrderTimeoutKeyedProcessFunction)

        orderProcessDS.getSideOutput(new OutputTag[String]("timeout"))//.print("timeout")
        //orderProcessDS
    }


    def analysesTimeout() = {
        // TODO 获取订单交易数据
        val dataDS: DataStream[String] = orderTimeoutApalysesDao.readTextFile("input/OrderLog.csv")
        val orderDS = dataDS.map(
            data => {
                val datas = data.split(",")
                OrderEvent(datas(0).toLong,datas(1),datas(2),datas(3).toLong)
            }
        )

        val orderTimeDS = orderDS.assignAscendingTimestamps(_.eventTime * 1000L)

        val orderKS: KeyedStream[OrderEvent, Long] = orderTimeDS.keyBy(_.orderId)

        // 定义规则
        val pattern =
            Pattern
                    .begin[OrderEvent]("begin")
                    .where(_.eventType == "create")
                    .followedBy("followed")
                    .where(_.eventType == "pay")
                    .within(Time.minutes(15))


        // 应用规则
        val orderPS: PatternStream[OrderEvent] = CEP.pattern(orderKS, pattern)

        // 获取结果
        // within可以在指定时间范围内定义规则。
        // select方法支持函数柯里化
        // 第一个参数列表表示侧输出流标签
        // 第二个参数列表表示超时数据的处理
        // 第三个参数列表表示正常数据的处理
        val outputTag = new OutputTag[String]("timeout")
        val selectDS = orderPS.select(
            outputTag
        )(
            (map, ts) => {
                map.toString
            }
        )(
            map => {
                val order = map("begin").iterator.next()
                val pay = map("followed").iterator.next()
                var s = "订单ID :" + order.orderId
                s += "共耗时 " +(pay.eventTime - order.eventTime)+ " 秒"
                s
            }
        )

        selectDS.getSideOutput(outputTag)
    }
}
