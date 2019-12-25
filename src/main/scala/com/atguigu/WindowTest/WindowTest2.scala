package com.atguigu.WindowTest

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.functions.{AssignerWithPeriodicWatermarks, AssignerWithPunctuatedWatermarks, KeyedProcessFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

object WindowTest2 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getConfig.setAutoWatermarkInterval(100L)

//    val stream = env.readTextFile("E:\\learn\\FlinkTutorial\\src\\main\\resources\\sensor.txt")

    val stream = env.socketTextStream("localhost",7777)

    val dataStream = stream.map(data =>{
      val dataArray = data.split(",")
      SensorReading1(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
    })
//      .assignTimestampsAndWatermarks( new MyAssigner2)
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading1](Time.seconds(1)) {
        override def extractTimestamp(element: SensorReading1): Long = element.timestamp * 1000
      })

    //统计15s内的最小温度, 隔5秒输出一次
    val minTemPerWindowStream = dataStream
      .map(data => (data.id, data.temperature))
      .keyBy(_._1)
      .timeWindow(Time.seconds(15), Time.seconds(5))  //滚动时间窗口
      .reduce((result, data) => (data._1, result._2.min(data._2))) //用reduce做增量聚合

    minTemPerWindowStream.print("min temp")
    dataStream.print("input data")

    dataStream.keyBy(_.id)
        .process(new MyProcess())

    env.execute("window test")
  }
}

class MyAssigner2 extends AssignerWithPeriodicWatermarks[SensorReading1]{
  val bound = 60000
  var maxTs = Long.MinValue

  override def getCurrentWatermark: Watermark = new Watermark(maxTs - bound)

  override def extractTimestamp(t: SensorReading1, l: Long): Long = {
    maxTs = maxTs.max(t.timestamp * 1000)
    t.timestamp * 1000
  }
}

class MyAssiginer3 extends AssignerWithPunctuatedWatermarks[SensorReading1]{
  override def checkAndGetNextWatermark(lastElement: SensorReading1, extractedTimestamp: Long): Watermark = new Watermark(extractedTimestamp)

  override def extractTimestamp(element: SensorReading1, previousElementTimestamp: Long): Long = element.timestamp * 1000
}

class MyProcess() extends KeyedProcessFunction[String,SensorReading1,String]{
  override def processElement(value: SensorReading1, ctx: KeyedProcessFunction[String, SensorReading1, String]#Context, out: Collector[String]): Unit = {
    ctx.timerService().registerEventTimeTimer(2000)
  }
}