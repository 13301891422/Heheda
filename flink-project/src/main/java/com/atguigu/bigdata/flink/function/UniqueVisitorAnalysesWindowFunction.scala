package com.atguigu.bigdata.flink.function

import java.sql.Timestamp

import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable

class UniqueVisitorAnalysesWindowFunction
    extends ProcessAllWindowFunction[(Long, Int), String, TimeWindow]{
    override def process(context: Context, elements: Iterable[(Long, Int)], out: Collector[String]): Unit = {

        // 将窗口中数据去重
        val set = mutable.Set[Long]()
        // TODO 内存使用 ？
        // 1byte => 8bit * 1024 =
        // Long => 4byte
        // 1000000
        // 100000 => char[] => 6 * 2 = 12byte
        // TODO 数据不是以全量数据的方式去重，而是以一个一个的方式去重
        // 内存？
        // 去重(判断用户是否存在)=>UV
        // user001
        // user002
        // user003
        // 0，1
        // 0 0 0 0 0 1 1 1

        val iterator: Iterator[(Long, Int)] = elements.iterator

        while (iterator.hasNext) {
            set.add( iterator.next()._1 )
        }

        val builder = new StringBuilder()
        builder.append("time : " + new Timestamp(context.window.getEnd) + "\n")
        builder.append("网站独立访客数 ： " + set.size + "\n")
        builder.append("===============================")

        out.collect(builder.toString())
    }
}
