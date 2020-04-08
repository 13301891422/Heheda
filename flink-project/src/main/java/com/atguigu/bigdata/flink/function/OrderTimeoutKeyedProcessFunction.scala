package com.atguigu.bigdata.flink.function

import com.atguigu.bigdata.flink.bean.{OrderEvent, OrderMergePay}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
  * 订单超时支付的数据处理函数
  */
class OrderTimeoutKeyedProcessFunction extends KeyedProcessFunction[Long, OrderEvent, String]{

    private var orderMergePay:ValueState[OrderMergePay] = _
    private var alarmTimer:ValueState[Long] = _


    override def open(parameters: Configuration): Unit = {
        orderMergePay = getRuntimeContext.getState(
            new ValueStateDescriptor[OrderMergePay]("orderMergePay", classOf[OrderMergePay])
        )
        alarmTimer = getRuntimeContext.getState(
            new ValueStateDescriptor[Long]("alarmTimer", classOf[Long])
        )
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, OrderEvent, String]#OnTimerContext, out: Collector[String]): Unit = {
        // 定时器如果触发，说明数据无法在指定时间范围内到达，做特殊处理
        val mergeData: OrderMergePay = orderMergePay.value()
        val outputTag = new OutputTag[String]("timeout")
        if ( mergeData.orderTS != 0 ) {
            ctx.output(outputTag, s"订单ID：${mergeData.orderId}15分钟内未收到支付数据，交易失败")
        } else {
            ctx.output(outputTag, s"订单ID：${mergeData.orderId} 未收到下单数据，交易有问题")
        }
        // 将状态数据进行清除
        orderMergePay.clear()
        alarmTimer.clear()
    }

    override def processElement(event: OrderEvent, ctx: KeyedProcessFunction[Long, OrderEvent, String]#Context, out: Collector[String]): Unit = {

        // 数据到来时，只有两种情况
        // create, pay
        // 数据有可能是乱序的，pay数据可能先到
        var mergeData = orderMergePay.value()
        val outputTag = new OutputTag[String]("timeout")
        if ( event.eventType == "create" ) {
            // create数据来的场合

            if ( mergeData == null ) {
                // pay数据没来, 应该等待pay的到来，将当前下单数据保存状态
                mergeData = OrderMergePay( event.orderId, event.eventTime, 0L )
                orderMergePay.update(mergeData)
                // 增加定时器，如果超出指定的时间，那么说明数据无法到达
                ctx.timerService().registerEventTimeTimer(event.eventTime * 1000 + 1000*60*15)
                // 如果使用EventTime,那么当pay的数据来的时候，会删除定时器，所以无法执行定时操作
                // 所以这里可以不采用EventTime的定时器，采用框架自身的处理时间定时器

//                ctx.timerService().registerProcessingTimeTimer(
//                    ctx.timerService().currentProcessingTime()
//                            + 1000*60*15)

                alarmTimer.update(event.eventTime * 1000 + 1000*60*15)
            } else {
                // pay数据已经来了
                val diff = math.abs(mergeData.payTS-event.eventTime)
                if ( diff > 900 ) {
                    var s = "订单ID ：" + mergeData.orderId
                    s += "交易支付超时，支付耗时"+diff+"秒"
                    ctx.output(outputTag, s)
                } else {
                    var s = "订单ID ：" + mergeData.orderId
                    s += "交易成功，支付耗时"+diff+"秒"
                    out.collect(s)
                }

                // 支付成功，删除定时器，并清除状态
                ctx.timerService().deleteEventTimeTimer(alarmTimer.value())
                orderMergePay.clear()
                alarmTimer.clear()
            }
        } else {
            // pay数据来的场合
            if ( mergeData == null ) {
                // create数据没来,应该等待create的到来，将当前下单数据保存状态
                mergeData = OrderMergePay( event.orderId, 0L, event.eventTime )
                orderMergePay.update(mergeData)
                ctx.timerService().registerEventTimeTimer(event.eventTime * 1000 + 1000*60*5)
                alarmTimer.update(event.eventTime * 1000 + 1000*60*5)
            } else {
                // create数据已经来了
                val diff = math.abs(event.eventTime-mergeData.orderTS)
                if ( diff > 900 ) {
                    var s = "订单ID ：" + mergeData.orderId
                    s += "交易支付超时，支付耗时"+diff+"秒"
                    ctx.output(outputTag, s)
                } else {
                    var s = "订单ID ：" + mergeData.orderId
                    s += "交易成功，支付耗时"+diff+"秒"
                    out.collect(s)
                }
                // 支付成功，删除定时器，并清除状态
                ctx.timerService().deleteEventTimeTimer(alarmTimer.value())
                orderMergePay.clear()
                alarmTimer.clear()
            }
        }

    }
}
