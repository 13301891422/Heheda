package com.atguigu.bigdata.flink.service

import java.text.SimpleDateFormat

import com.atguigu.bigdata.flink.bean
import com.atguigu.bigdata.flink.bean.ApacheLog
import com.atguigu.bigdata.flink.common.{TDao, TService}
import com.atguigu.bigdata.flink.dao.HotResourcesAnalysesDao
import com.atguigu.bigdata.flink.function.{HotResourcesKeyedProcessFunction, HotResourcesWindowFunction, MyAggregateFunction}
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

/**
  * 热门资源浏览量服务
  */
class HotResourcesAnalysesService extends TService {
    private val hotResourcesAnalysesDao = new HotResourcesAnalysesDao

    override def getDao(): TDao = hotResourcesAnalysesDao

    override def analyses() = {
        // TODO 获取服务器日志数据
        val dataDS: DataStream[String] = hotResourcesAnalysesDao.readTextFile("input/apache.log")

        // TODO 将数据进行封装
        val logDS: DataStream[ApacheLog] = dataDS.map(
            log => {
                val datas: Array[String] = log.split(" ")
                val sdf = new SimpleDateFormat("dd/MM/yy:HH:mm:ss")
                ApacheLog(
                    datas(0),
                    datas(1),
                    sdf.parse(datas(3)).getTime,
                    datas(5),
                    datas(6)
                )
            }
        )

        // TODO 抽取事件时间和水位线标记
        val waterDS: DataStream[ApacheLog] = logDS.assignTimestampsAndWatermarks(
            new BoundedOutOfOrdernessTimestampExtractor[ApacheLog](Time.minutes(1)) {
                override def extractTimestamp(element: ApacheLog): Long = {
                    element.eventTime
                }
            }
        )

        // TODO 根据资源访问路径进行分组
        val logKS: KeyedStream[ApacheLog, String] = waterDS.keyBy(_.url)

        // TODO 增加窗口数据范围。
        val logWS: WindowedStream[ApacheLog, String, TimeWindow] =
            logKS.timeWindow(Time.minutes(10), Time.seconds(5))

        // TODO 将数据进行聚合
        val aggDS: DataStream[bean.HotResourceClick] = logWS.aggregate(
            //new HotResourcesAggregateFunction
            new MyAggregateFunction[ApacheLog],
            new HotResourcesWindowFunction
        )

        // TODO 根据窗口时间将数据重新进行分组
        val hrcKS: KeyedStream[bean.HotResourceClick, Long] = aggDS.keyBy(_.windowEndTime)

        // TODO 将分组后的数据处理
        hrcKS.process( new HotResourcesKeyedProcessFunction )

    }
}
