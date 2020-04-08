package com.atguigu.bigdata.flink.service

import com.atguigu.bigdata.flink.bean.{AdClickLog, CountByProvince}
import com.atguigu.bigdata.flink.common.{TDao, TService}
import com.atguigu.bigdata.flink.dao.AdvertClickAnalysesDao
import com.atguigu.bigdata.flink.function.AdvClickKeyedProcessFunction
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

class AdvertClickAnalysesService extends TService {

    private val advertClickAnalysesDao = new AdvertClickAnalysesDao

    override def getDao(): TDao = advertClickAnalysesDao

    /**
      * 广告点击统计
      * @return
      */
    def analysesAdv() = {

        // 获取广告点击数据
        val dataDS: DataStream[String] = advertClickAnalysesDao.readTextFile("input/AdClickLog.csv")

        // 将数据进行封装
        val logDS: DataStream[AdClickLog] = dataDS.map(
            data => {
                val datas = data.split(",")
                AdClickLog(
                    datas(0).toLong,
                    datas(1).toLong,
                    datas(2),
                    datas(3),
                    datas(4).toLong
                )
            }
        )

        // 抽取时间戳和水位线标记
        val timeDS: DataStream[AdClickLog] = logDS.assignAscendingTimestamps( _.timestamp * 1000L )

        // ((province, adv) - Count)
        // 将数据进行统计
        val ds: DataStream[CountByProvince] = timeDS
                .map(
                    log => {
                        (log.province + "_" + log.adId, 1L)
                    }
                )
                .keyBy(_._1)
                .timeWindow(Time.hours(1), Time.seconds(5))
                .aggregate(
                    new AggregateFunction[(String, Long), Long, Long] {
                        override def createAccumulator(): Long = 0L

                        override def add(value: (String, Long), accumulator: Long): Long =
                            accumulator + 1L

                        override def getResult(accumulator: Long): Long = accumulator

                        override def merge(a: Long, b: Long): Long = a + b
                    },
                    new WindowFunction[Long, CountByProvince, String, TimeWindow] {
                        override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[CountByProvince]): Unit = {
                            val ks = key.split("_")
                            val count = input.iterator.next
                            out.collect(
                                CountByProvince(
                                    window.getEnd.toString,
                                    ks(0),
                                    ks(1).toLong,
                                    count
                                )
                            )
                        }
                    }
                )

        //val value: KeyedStream[CountByProvince, Tuple] = ds.keyBy(0)
        //val value: KeyedStream[CountByProvince, Tuple] = ds.keyBy("windowEnd")
        //val value: KeyedStream[CountByProvince, String] = ds.keyBy(t=>t.windowEnd)
        //        ds.keyBy(
        //            new KeySelector[CountByProvince, String] {
        //                override def getKey(value: CountByProvince): String = {
        //                    value.windowEnd
        //                }
        //            }
        //        )

        val dataKS: KeyedStream[CountByProvince, String] = ds.keyBy(t=>t.windowEnd)

        val dataStream: DataStream[String] = dataKS.process(
            new KeyedProcessFunction[String, CountByProvince, String] {
                override def processElement(value: CountByProvince, ctx: KeyedProcessFunction[String, CountByProvince, String]#Context, out: Collector[String]): Unit = {
                    //                    if (value.count >= 100) {
                    //                        val outputTag = new OutputTag[CountByProvince]("blackList")
                    //                        ctx.output(outputTag, value)
                    //                    } else {
                    //
                    //                    }
                    out.collect("省份：" + value.province + ", 广告：" + value.adId + ",点击数量: " + value.count)
                }
            }
        )
        //        val outputTag = new OutputTag[CountByProvince]("blackList")
        //        val sideDS: DataStream[CountByProvince] = dataStream.getSideOutput(outputTag)
        //        sideDS.print("blackList>>>>>>")
        dataStream
    }

    override def analyses() = {

        //analysesAdv

        analysesAdv4BlackList
    }

    /**
      * 广告统计黑名单处理
      */
    def analysesAdv4BlackList() = {
        // 获取广告点击数据
        val dataDS: DataStream[String] = advertClickAnalysesDao.readTextFile("input/AdClickLog.csv")

        // 将数据进行封装
        val logDS: DataStream[AdClickLog] = dataDS.map(
            data => {
                val datas = data.split(",")
                AdClickLog(
                    datas(0).toLong,
                    datas(1).toLong,
                    datas(2),
                    datas(3),
                    datas(4).toLong
                )
            }
        )

        // 抽取时间戳和水位线标记
        val timeDS: DataStream[AdClickLog] = logDS.assignAscendingTimestamps( _.timestamp * 1000L )

        // ((province, adv) - Count)
        // 将数据进行统计
        val logKS: KeyedStream[(String, Long), String] = timeDS
                .map(
                    log => {
                        (log.province + "_" + log.adId, 1L)
                    }
                )
                .keyBy(_._1)

        val blackListDS: DataStream[(String, Long)] =
            logKS.process(new AdvClickKeyedProcessFunction)

        // 黑名单的侧输出流
        val outputTag = new OutputTag[(String, Long)]("blackList")
        blackListDS.getSideOutput(outputTag).print("blackList>>>")
        // 正常数据流
        val normalDS = blackListDS.keyBy(_._1)
            .timeWindow(Time.hours(1), Time.seconds(5))
                .aggregate(
                    new AggregateFunction[(String, Long), Long, Long] {
                        override def createAccumulator(): Long = 0L

                        override def add(value: (String, Long), accumulator: Long): Long =
                            accumulator + 1L

                        override def getResult(accumulator: Long): Long = accumulator

                        override def merge(a: Long, b: Long): Long = a + b
                    },
                    new WindowFunction[Long, CountByProvince, String, TimeWindow] {
                        override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[CountByProvince]): Unit = {
                            val ks = key.split("_")
                            val count = input.iterator.next
                            out.collect(
                                CountByProvince(
                                    window.getEnd.toString,
                                    ks(0),
                                    ks(1).toLong,
                                    count
                                )
                            )
                        }
                    }
        )

        val dataKS: KeyedStream[CountByProvince, String] = normalDS.keyBy(t=>t.windowEnd)

        val dataStream: DataStream[String] = dataKS.process(
            new KeyedProcessFunction[String, CountByProvince, String] {
                override def processElement(value: CountByProvince, ctx: KeyedProcessFunction[String, CountByProvince, String]#Context, out: Collector[String]): Unit = {
                    out.collect("省份：" + value.province + ", 广告：" + value.adId + ",点击数量: " + value.count)
                }
            }
        )
        dataStream
    }
}
