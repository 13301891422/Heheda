package com.atguigu.bigdata.flink.service

import com.atguigu.bigdata.flink.bean
import com.atguigu.bigdata.flink.common.{TDao, TService}
import com.atguigu.bigdata.flink.dao.UniqueVisitorAnalysesDao
import com.atguigu.bigdata.flink.function.{UniqueVisitorAnalusesByBloomFilterWindowFunction, UniqueVisitorAnalysesWindowFunction}
import org.apache.flink.streaming.api.scala.{DataStream, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

class UniqueVisitorAnalysesService extends TService {

    private val uniqueVisitorAnalysesDao = new UniqueVisitorAnalysesDao

    override def getDao(): TDao = uniqueVisitorAnalysesDao

    def uvAnalyses() = {

        // TODO 获取用户行为数据
        val dataDS: DataStream[bean.UserBehavior] = getUserBehaviorDatas()

        // TODO 抽取时间戳和水位线标记
        val waterDS: DataStream[bean.UserBehavior] = dataDS.assignAscendingTimestamps(_.timestamp * 1000L)

        // TODO 将数据进行结构的转换
        val userDS: DataStream[(Long, Int)] = waterDS.map(
            data => {
                (data.userId, 1)
            }
        )

        // TODO 设定窗口范围
        // 将所有的数据放入一个窗口中
        val dataWS: AllWindowedStream[(Long, Int), TimeWindow] = userDS.timeWindowAll(Time.hours(1))

        // TODO 判断一个小时窗口中不重复的用户ID的个数
        dataWS.process( new UniqueVisitorAnalysesWindowFunction )
    }

    def uvAnalysesByBloomFilter() = {
        // TODO 获取用户行为数据
        val dataDS: DataStream[bean.UserBehavior] = getUserBehaviorDatas()

        // TODO 抽取时间戳和水位线标记
        val waterDS: DataStream[bean.UserBehavior] = dataDS.assignAscendingTimestamps(_.timestamp * 1000L)

        // TODO 将数据进行结构的转换
        val userDS: DataStream[(Long, Int)] = waterDS.map(
            data => {
                (data.userId, 1)
            }
        )

        // TODO 设定窗口范围
        // 将所有的数据放入一个窗口中
        val dataWS: AllWindowedStream[(Long, Int), TimeWindow] = userDS.timeWindowAll(Time.hours(1))

        // 不能使用全量数据处理，因为会将窗口所有数据放置在内存中
        //dataWS.process()
        // 不能使用累加器，因为累加器一般只应用于单独数据累加，不做业务逻辑处理
        //dataWS.aggregate()

        // 希望能够一个一个数据进行业务逻辑处理
        // 可以使用window的触发器
        dataWS.trigger(
            new Trigger[(Long, Int), TimeWindow]() {
                override def onElement(element: (Long, Int), timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
                    // 开始计算,计算完毕后，将数据从窗口中清除
                    TriggerResult.FIRE_AND_PURGE
                }

                override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
                    TriggerResult.CONTINUE
                }

                override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
                    TriggerResult.CONTINUE
                }

                override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {

                }
            }
        ).process(
            new UniqueVisitorAnalusesByBloomFilterWindowFunction
        )
    }

    override def analyses() = {
        // UV统计（常规方式）
        //uvAnalyses

        // UV统计（布隆过滤器）
        uvAnalysesByBloomFilter
    }
}
