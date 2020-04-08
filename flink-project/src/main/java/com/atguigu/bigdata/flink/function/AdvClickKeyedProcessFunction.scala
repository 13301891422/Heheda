package com.atguigu.bigdata.flink.function

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

class AdvClickKeyedProcessFunction extends KeyedProcessFunction[String, (String, Long), (String, Long)] {

    private var advClickCount:ValueState[Long] = _
    private var alarmStatus:ValueState[Boolean] = _

    override def open(parameters: Configuration): Unit = {
        advClickCount = getRuntimeContext.getState(
            new ValueStateDescriptor[Long] ("advClickCount", classOf[Long])
        )
        alarmStatus = getRuntimeContext.getState(
            new ValueStateDescriptor[Boolean] ("alarmStatus", classOf[Boolean])
        )
    }


    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, (String, Long), (String, Long)]#OnTimerContext, out: Collector[(String, Long)]): Unit = {
        advClickCount.clear()
        alarmStatus.clear()
    }

    override def processElement(value: (String, Long), ctx: KeyedProcessFunction[String, (String, Long), (String, Long)]#Context, out: Collector[(String, Long)]): Unit = {
        val oldCount = advClickCount.value()
        // 增加一天的概念来进行处理
        // 可以增加定时器，当第二天的时候，应该将前一天的状态清空，重新计算
        // 如何触发定时器？
        // 1. 取得第一条数据的处理时间
        if ( oldCount == 0 ) {
            // 2020-03-07 16:20:21
            val currentTime: Long = ctx.timerService().currentProcessingTime()
            val day = currentTime / (1000 * 60 * 60 * 24 )
            // 2. 根据处理时间计算得出第二天的起始时间，重置状态，触发定时器
            // 2020-03-08 00:00:00
            val nextday = day + 1
            val nextTimestamp = nextday * (1000 * 60 * 60 * 24 )
            ctx.timerService().registerProcessingTimeTimer(nextTimestamp)
        }

        val newCount = oldCount + 1

        if ( newCount >= 100 ) {
            if ( !alarmStatus.value() ) {
                // 如果数据统计结果超过阈值，那么将数据输出到侧输出流
                val outputTag = new OutputTag[(String, Long)]("blackList")
                ctx.output( outputTag, value )
                // 更新提示状态
                alarmStatus.update(true)
            }
        } else {
            // 如果数据统计结果没有超过阈值，那么直接输出到正常流中
            out.collect(value)
        }

        advClickCount.update(newCount)
    }
}
