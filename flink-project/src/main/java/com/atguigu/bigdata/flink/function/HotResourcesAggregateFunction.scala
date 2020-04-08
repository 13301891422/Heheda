package com.atguigu.bigdata.flink.function

import com.atguigu.bigdata.flink.bean.{ApacheLog}
import org.apache.flink.api.common.functions.AggregateFunction

/**
  * 热门资源累加函数
  */
class HotResourcesAggregateFunction extends AggregateFunction[ApacheLog, Long, Long] {
    override def createAccumulator(): Long = 0L

    override def add(value: ApacheLog, accumulator: Long): Long = {
        accumulator + 1L
    }

    override def getResult(accumulator: Long): Long = accumulator

    override def merge(a: Long, b: Long): Long = a + b
}
