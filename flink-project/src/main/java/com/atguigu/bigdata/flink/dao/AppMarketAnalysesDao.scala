package com.atguigu.bigdata.flink.dao

import com.atguigu.bigdata.flink.bean.MarketingUserBehavior
import com.atguigu.bigdata.flink.common.TDao
import com.atguigu.bigdata.flink.util.FlinkStreamEnv
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._

class AppMarketAnalysesDao extends TDao{

    /**
      * 生成模拟数据
      */
    def mockData() = {
        // 创建自定义数据源
        FlinkStreamEnv.get().addSource(
            new SourceFunction[MarketingUserBehavior]() {
                var runflg = true
                override def run(ctx: SourceFunction.SourceContext[MarketingUserBehavior]): Unit = {
                    while ( runflg ) {
                        ctx.collect(MarketingUserBehavior(
                            1,
                            "INSTALL",
                            "HUAWEI",
                            System.currentTimeMillis()
                        ))
                        Thread.sleep(500)
                    }
                }

                override def cancel(): Unit = {
                    runflg = false
                }
            }
        )
    }
}
