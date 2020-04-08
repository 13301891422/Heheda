package com.atguigu.bigdata.flink.service

import com.atguigu.bigdata.flink.bean.LoginEvent
import com.atguigu.bigdata.flink.common.{TDao, TService}
import com.atguigu.bigdata.flink.dao.LoginFailAnalysesDao
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.{KeyedProcessFunction, ProcessFunction}
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

class LoginFailAnalysesService extends TService {
    private val loginFailAnalysesDao = new LoginFailAnalysesDao
    override def getDao(): TDao = loginFailAnalysesDao

    def analysesNormal() = {

        // TODO 恶意登陆逻辑上的问题：
        // 1. 最开始逻辑就有问题,不应该过滤登陆成功的数据
        // 2. 如果连续的两条数据是乱序的，统计有问题
        // 3. 如果登陆失败的次数大于2次，该如何处理？

        val dataDS: DataStream[String] = loginFailAnalysesDao.readTextFile("input/LoginLog.csv")
        val loginDS: DataStream[LoginEvent] = dataDS.map(
            data => {
                val datas = data.split(",")
                LoginEvent(
                    datas(0).toLong,
                    datas(1),
                    datas(2),
                    datas(3).toLong
                )
            }
        )
        val timeDS: DataStream[LoginEvent] = loginDS.assignTimestampsAndWatermarks(
            new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(10)) {
                override def extractTimestamp(element: LoginEvent): Long = {
                    element.eventTime * 1000L
                }
            }
        )
        // KeyedStream中可以使用状态类型数据：
        // ValueState
        // ListState
        // MapState
        timeDS
                .filter(_.eventType == "fail")
                .keyBy(_.userId)
                .process(
                    new KeyedProcessFunction[Long, LoginEvent, String] {

                        private var lastLoginEvent : ValueState[LoginEvent] = _

                        override def open(parameters: Configuration): Unit = {
                            lastLoginEvent = getRuntimeContext.getState(
                                new ValueStateDescriptor[LoginEvent]("lastLoginEvent", classOf[LoginEvent])
                            )
                        }

                        override def processElement(value: LoginEvent, ctx: KeyedProcessFunction[Long, LoginEvent, String]#Context, out: Collector[String]): Unit = {
                            val lastEvent = lastLoginEvent.value()
                            if ( lastEvent != null ) {
                                if (  value.eventTime - lastEvent.eventTime <= 2 ) {
                                    out.collect(value.userId + "在连续2秒内登陆失败2次")
                                }
                            }

                            lastLoginEvent.update(value)
                        }
                    }
                )
    }

    override def analyses() = {
        //analysesNormal

        // 使用CEP进行逻辑的处理
        analysesWithCEP
    }
    def analysesWithCEP() = {
        val dataDS: DataStream[String] = loginFailAnalysesDao.readTextFile("input/LoginLog.csv")
        val loginDS: DataStream[LoginEvent] = dataDS.map(
            data => {
                val datas = data.split(",")
                LoginEvent(
                    datas(0).toLong,
                    datas(1),
                    datas(2),
                    datas(3).toLong
                )
            }
        )
        val timeDS: DataStream[LoginEvent] = loginDS.assignTimestampsAndWatermarks(
            new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(10)) {
                override def extractTimestamp(element: LoginEvent): Long = {
                    element.eventTime * 1000L
                }
            }
        )

        val userKS: KeyedStream[LoginEvent, Long] = timeDS.keyBy(_.userId)

        // 定义规则
        val pattern = Pattern
                .begin[LoginEvent]("begin")
                .where(_.eventType == "fail")
                .next("next")
                .where(_.eventType == "fail")
                // 定义时间范围
                .within( Time.seconds(5) )

        // 应用规则
        val userLoginPS: PatternStream[LoginEvent] = CEP.pattern(userKS, pattern)

        // 获取结果
        userLoginPS.select(
            map => {
                map.toString
            }
        )
    }
}
