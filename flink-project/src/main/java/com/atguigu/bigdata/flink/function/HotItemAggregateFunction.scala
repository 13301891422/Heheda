package com.atguigu.bigdata.flink.function

import com.atguigu.bigdata.flink.bean.UserBehavior
import org.apache.flink.api.common.functions.AggregateFunction

/**
  * 热门商品累加函数
  */
class HotItemAggregateFunction extends AggregateFunction[UserBehavior, Long, Long] {
    override def createAccumulator(): Long = 0L

    override def add(value: UserBehavior, accumulator: Long): Long = {
        accumulator + 1L
    }

    override def getResult(accumulator: Long): Long = accumulator

    override def merge(a: Long, b: Long): Long = a + b
}
