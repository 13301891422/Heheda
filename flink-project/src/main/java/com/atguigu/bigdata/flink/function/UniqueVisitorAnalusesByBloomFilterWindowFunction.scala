package com.atguigu.bigdata.flink.function

import java.lang
import java.sql.Timestamp

import com.atguigu.bigdata.flink.util.MockBloomFilter
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import redis.clients.jedis.Jedis

class UniqueVisitorAnalusesByBloomFilterWindowFunction
    extends ProcessAllWindowFunction[(Long, Int), String, TimeWindow]{

    private var jedis : Jedis = _

    override def open(parameters: Configuration): Unit = {
        jedis = new Jedis("linux4", 6379)
    }

    override def process(context: Context, elements: Iterable[(Long, Int)], out: Collector[String]): Unit = {

        val bitMapKey = context.window.getEnd.toString

        // 获取用户ID
        val userId = elements.iterator.next()._1.toString

        // 获取用户ID在位图的偏移量
        val offset: Long = MockBloomFilter.offset( userId, 25 )

        // 根据偏移量判断用户ID在redis位图中是否存在
        val boolean: lang.Boolean = jedis.getbit(bitMapKey, offset)
        if ( boolean ) {
            // 如果位图中已经存在，那么什么都不做
        } else {
            // 如果位图中不存在

            // 更新状态
            jedis.setbit(bitMapKey, offset, true)
            // 增加UV数量
            val uv: String = jedis.hget("uvcount", bitMapKey)
            var uvCount : Long = 0
            if ( uv != null && "" != uv ) {
                uvCount = uv.toLong
            }
            jedis.hset("uvcount", bitMapKey, (uvCount + 1).toString)
            out.collect(new Timestamp(context.window.getEnd) + "新的独立访客 = " + userId)
        }
        //






        //out.collect("size = " + elements.size)
    }
}
