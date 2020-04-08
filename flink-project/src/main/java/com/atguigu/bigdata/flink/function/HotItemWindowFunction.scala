package com.atguigu.bigdata.flink.function

import com.atguigu.bigdata.flink.bean.{HotItemClick, UserBehavior}
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
  * 热门商品窗口处理函数
  */
class HotItemWindowFunction extends WindowFunction[ Long, HotItemClick, Long, TimeWindow]{
    override def apply(key: Long, window: TimeWindow, input: Iterable[Long], out: Collector[HotItemClick]): Unit = {
        out.collect( HotItemClick( key, input.iterator.next(), window.getEnd ) )
    }
}
