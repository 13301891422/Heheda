package com.atguigu.bigdata.flink.service

import com.atguigu.bigdata.flink.bean
import com.atguigu.bigdata.flink.bean.UserBehavior
import com.atguigu.bigdata.flink.common.{TDao, TService}
import com.atguigu.bigdata.flink.dao.HotItemAnalysesDao
import com.atguigu.bigdata.flink.function.{HotItemAggregateFunction, HotItemProcessFunction, HotItemWindowFunction}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

/**
  * 热门商品分析
  */
class HotItemAnalysesService extends TService {

    private val hotItemAnalysesDao = new HotItemAnalysesDao();

    override def getDao(): TDao = hotItemAnalysesDao

    /**
      * 数据分析
      */
    override def analyses() = {

        // TODO 1. 获取用户行为数据
        val ds: DataStream[UserBehavior] = getUserBehaviorDatas()
        //val ds: DataStream[UserBehavior] = getUserBehaviorDatas()

        // TODO 2. 设置时间戳和水位线标记
        val timeDS: DataStream[UserBehavior] = ds.assignAscendingTimestamps( _.timestamp * 1000L )

        // TODO 3. 将数据进行清洗，保留点击数据
        val filterDS: DataStream[UserBehavior] = timeDS.filter( _.behavior == "pv" )

        // TODO 4. 将相同的商品聚合在一起
        val dataKS: KeyedStream[UserBehavior, Long] = filterDS.keyBy(_.itemId)

        // TODO 5. 设定时间窗口
        val dataWS: WindowedStream[UserBehavior, Long, TimeWindow] =
            dataKS.timeWindow(Time.hours(1), Time.minutes(5))
        // TODO 6. 聚合数据
        //dataWS.sum()
        //dataWS.reduce()
        // 单一数据处理
        //val value: DataStream[Nothing] = dataWS.aggregate(null)
        // 窗口全量数据处理
        //val value: DataStream[Nothing] = dataWS.process(null)

        // 6.1 聚合数据
        // 6.2 将聚合的结果进行转换，方便排序
        // aggregate函数需要两个参数
        // 第一个参数表示聚合函数
        // 第二个参数表示当前窗口的处理函数
        // 第一个函数的处理结果会作为第二个函数输入值进行传递


        // 当窗口数据进行聚合后，会将所有窗口数据全部打乱，获取总的数据
        val hicDS: DataStream[bean.HotItemClick] = dataWS.aggregate(
            new HotItemAggregateFunction,
            new HotItemWindowFunction
        )

        // TODO 7. 将数据根据窗口重新分组
        val hicKS: KeyedStream[bean.HotItemClick, Long] = hicDS.keyBy(_.windowEndTime)



        // TODO 8. 对聚合后的数据进行排序
        val result: DataStream[String] = hicKS.process(new HotItemProcessFunction)

        result
    }
}
