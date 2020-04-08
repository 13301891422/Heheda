package com.atguigu.bigdata.flink.util

/**
  * 布隆过滤器模拟对象
  * 使用redis作为位图 ： key(id)  field(offset) value(0,1)
  * 使用当前对象对位图进行定位处理
  */
object MockBloomFilter {

    // 位图的容量
    // Redis位图应该最大为512 * 1024 * 1024 * 8
    val cap = 1 << 29

    def main(args: Array[String]): Unit = {

        println(offset("abc", 5))
        println(offset("abc", 10))

    }

    // HashMap => hash(key.hashCode) & (length - 1) => index
    // Redis   => crc(key.hashCode)  & (16384 - 1)  => slot
    def offset( s : String, seed:Int ) : Long = {
        var hash = 0

        for ( c <- s ) {
            hash = hash * seed + c
        }

        // 将数据进行散列后计算位图偏移量
        hash & (cap-1)
    }
}
