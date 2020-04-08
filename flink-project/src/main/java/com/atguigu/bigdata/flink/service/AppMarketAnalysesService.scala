package com.atguigu.bigdata.flink.service

import com.atguigu.bigdata.flink.bean
import com.atguigu.bigdata.flink.common.{TDao, TService}
import com.atguigu.bigdata.flink.dao.AppMarketAnalysesDao
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

class AppMarketAnalysesService extends TService{

    private val appMarketAnalysesDao = new AppMarketAnalysesDao

    override def getDao(): TDao = appMarketAnalysesDao

    override def analyses() = {
        // 获取市场渠道推广数据
        val dataDS: DataStream[bean.MarketingUserBehavior] = appMarketAnalysesDao.mockData()

        val timeDS = dataDS.assignAscendingTimestamps( _.timestamp )

        // 将数据进行统计 ：( channel + 用户行为, 1 )
        val kvDS: DataStream[(String, Int)] = timeDS.map(
            data => {
                (data.channel + "_" + data.behavior, 1)
            }
        )

        val result: DataStream[String] = kvDS
                .keyBy(_._1)
                .timeWindow(Time.minutes(1), Time.seconds(5))
                // 求和，但是返回的结果其实就是 (word, totalcount)
                //.sum()
                // 求和，可以自定义输出内容，包括窗口信息
                .process(
                    new ProcessWindowFunction[(String, Int), String, String, TimeWindow] {
                        override def process(key: String, context: Context, elements: Iterable[(String, Int)],
                                             out: Collector[String]): Unit = {
                            out.collect(context.window.getStart + " - "
                              + context.window.getEnd + ",APP安装量 = " + elements.size)
                        }
                    }
                )
        result
    }
}
