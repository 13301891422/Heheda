package com.atguigu.bigdata.flink.function

import com.atguigu.bigdata.flink.bean.{HotItemClick, HotResourceClick}
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
  * 热门资源窗口处理函数
  */
class HotResourcesWindowFunction extends WindowFunction[ Long, HotResourceClick, String, TimeWindow]{
    override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[HotResourceClick]): Unit = {
        out.collect( HotResourceClick( key, input.iterator.next(), window.getEnd ) )
    }
}
