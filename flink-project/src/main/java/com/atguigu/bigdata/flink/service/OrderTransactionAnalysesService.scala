package com.atguigu.bigdata.flink.service

import com.atguigu.bigdata.flink.bean.{OrderEvent, TxEvent}
import com.atguigu.bigdata.flink.common.{TDao, TService}
import com.atguigu.bigdata.flink.dao.OrderTransactionAnalysesDao
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.{CoProcessFunction, ProcessJoinFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

class OrderTransactionAnalysesService extends TService{

    private val orderTransactionAnalysesDao = new OrderTransactionAnalysesDao

    override def getDao(): TDao = orderTransactionAnalysesDao

    def analysesNormal() = {

        // TODO 获取两条不同的数据流
        val dataDS1 = orderTransactionAnalysesDao.readTextFile("input/OrderLog.csv")
        val dataDS2 = orderTransactionAnalysesDao.readTextFile("input/ReceiptLog.csv")

        val orderDS = dataDS1.map(
            data => {
                val datas = data.split(",")
                OrderEvent( datas(0).toLong, datas(1), datas(2), datas(3).toLong)
            }
        )
        val orderTimeDS: DataStream[OrderEvent] = orderDS.assignAscendingTimestamps(_.eventTime * 1000)
        val orderKS: KeyedStream[OrderEvent, String] = orderTimeDS.keyBy(_.txId)

        var txDS = dataDS2.map(
            data => {
                val datas = data.split(",")
                TxEvent( datas(0), datas(1), datas(2).toLong )
            }
        )

        val txTimeDS: DataStream[TxEvent] = txDS.assignAscendingTimestamps(_.eventTime * 1000)
        val txKS: KeyedStream[TxEvent, String] = txTimeDS.keyBy(_.txId)

        // 将不同数据源的流连接在一起
        orderKS.connect(txKS).process(
            new CoProcessFunction[OrderEvent, TxEvent, String] {

                private var orderMap:MapState[String, String] = _
                private var txMap:MapState[String, String] = _

                override def open(parameters: Configuration): Unit = {
                    orderMap = getRuntimeContext.getMapState(
                        new MapStateDescriptor[String, String]("orderMap", classOf[String], classOf[String])
                    )
                    txMap = getRuntimeContext.getMapState(
                        new MapStateDescriptor[String, String]("txMap", classOf[String], classOf[String])
                    )
                }

                override def processElement1(value: OrderEvent, ctx: CoProcessFunction[OrderEvent, TxEvent, String]#Context, out: Collector[String]): Unit = {

                    // 订单数据来了
                    // 判断对账信息是否存在
                    val s: String = txMap.get(value.txId)
                    if( s == null ) {
                        // 没有对账信息
                        orderMap.put(value.txId, "order")
                        // 如果对账信息长时间不来，超过了阈值，那么会触发定时器
                        // 然后使用侧输出流来提示交易有问题
                        //ctx.timerService().registerProcessingTimeTimer()
                    } else {
                        out.collect("交易ID：" + value.txId + "对账完成")
                        txMap.remove(value.txId)
                    }
                }

                override def processElement2(value: TxEvent, ctx: CoProcessFunction[OrderEvent, TxEvent, String]#Context, out: Collector[String]): Unit = {
                    // 交易数据来了
                    val s: String = orderMap.get(value.txId)
                    if ( s == null ) {
                        // 没有订单数据
                        txMap.put(value.txId, "tx")
                    } else {
                        out.collect("交易ID：" + value.txId + "对账完成")
                        orderMap.remove(value.txId)
                    }
                }
            }
        )
    }

    override def analyses() = {
        //analysesNormal

        analysesJoin
    }

    def analysesJoin() = {

        // 使用Join处理方式对两个流进行连接处理
        val dataDS1 = orderTransactionAnalysesDao.readTextFile("input/OrderLog.csv")
        val dataDS2 = orderTransactionAnalysesDao.readTextFile("input/ReceiptLog.csv")

        val orderDS = dataDS1.map(
            data => {
                val datas = data.split(",")
                OrderEvent( datas(0).toLong, datas(1), datas(2), datas(3).toLong)
            }
        )
        val orderTimeDS: DataStream[OrderEvent] = orderDS.assignAscendingTimestamps(_.eventTime * 1000)
        val orderKS: KeyedStream[OrderEvent, String] = orderTimeDS.keyBy(_.txId)

        var txDS = dataDS2.map(
            data => {
                val datas = data.split(",")
                TxEvent( datas(0), datas(1), datas(2).toLong )
            }
        )

        val txTimeDS: DataStream[TxEvent] = txDS.assignAscendingTimestamps(_.eventTime * 1000)
        val txKS: KeyedStream[TxEvent, String] = txTimeDS.keyBy(_.txId)

        orderKS
                .intervalJoin(txKS)
                .between( Time.minutes(-5), Time.minutes(5) )
                .process(
                    new ProcessJoinFunction[OrderEvent, TxEvent, (OrderEvent, TxEvent)] {
                        override def processElement(left: OrderEvent, right: TxEvent, ctx: ProcessJoinFunction[OrderEvent, TxEvent, (OrderEvent, TxEvent)]#Context, out: Collector[(OrderEvent, TxEvent)]): Unit = {
                            out.collect(left, right)
                        }
                    }
                )
    }
}
