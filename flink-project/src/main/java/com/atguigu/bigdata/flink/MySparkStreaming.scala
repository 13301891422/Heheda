package com.atguigu.bigdata.flink

import org.apache.spark.SparkConf

object MySparkStreaming {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("MySparkStreaming")
  }
}
