package com.atguigu.bigdata.flink.service

import com.atguigu.bigdata.flink.bean
import com.atguigu.bigdata.flink.bean.UserBehavior
import com.atguigu.bigdata.flink.common.{TDao, TService}
import com.atguigu.bigdata.flink.dao.PageViewAnalysesDao
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

class PageViewAnalysesService extends TService {

    private val  pageViewAnalysesDao= new PageViewAnalysesDao

    override def getDao(): TDao = pageViewAnalysesDao

    override def analyses() = {

        // TODO 获取用户行为数据
        val dataDS: DataStream[bean.UserBehavior] = getUserBehaviorDatas()

        val timeDS: DataStream[UserBehavior] = dataDS.assignAscendingTimestamps( _.timestamp * 1000L )

        // TODO 将数据进行过滤，保留pv数据
        val pvDS: DataStream[bean.UserBehavior] = timeDS.filter(_.behavior == "pv")

        // TODO 将过滤后的数据进行结构的转换 ： （pv, 1）
        val pvToOneDS: DataStream[(String, Int)] = pvDS.map(pv=>("pv", 1))

        // TODO 将相同的key进行分组
        val pvToOneKS: KeyedStream[(String, Int), String] = pvToOneDS.keyBy(_._1)

        // TODO 设定窗口范围
        val pvToOneWS: WindowedStream[(String, Int), String, TimeWindow] = pvToOneKS.timeWindow(Time.hours(1))

        // TODO 数据聚合
        val pvToSumDS: DataStream[(String, Int)] = pvToOneWS.sum(1)

        pvToSumDS
    }
}
