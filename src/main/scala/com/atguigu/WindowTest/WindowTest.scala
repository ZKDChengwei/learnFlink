package com.atguigu.WindowTest



import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.functions.{AssignerWithPeriodicWatermarks, AssignerWithPunctuatedWatermarks}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time

//温度传感器读数样例类
case class SensorReading1( id:String, timestamp:Long, temperature:Double )
object WindowTest {
  def main(args: Array[String]): Unit = {
    val  env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getConfig.setAutoWatermarkInterval(100L)


//    val stream=env.readTextFile("E:\\learn\\FlinkTutorial\\src\\main\\resources\\sensor.txt")
//
    val stream = env.socketTextStream("localhost",7777)


    val dataStream = stream.map(data =>{
      val dataArray = data.split(",")
      SensorReading1(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
    })
//      .assignAscendingTimestamps(_.timestamp*1000)
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading1](Time.seconds(1)) {
        override def extractTimestamp(t: SensorReading1): Long = t.timestamp * 1000
      })

    val minTempPerWindowStream = dataStream
      .map(data => (data.id, data.temperature))
      .keyBy(_._1)
      .timeWindow(Time.seconds(10), Time.seconds(5))  //开时间窗口(滚动时间窗口)
      .reduce( (data1, data2) => (data1._1, data1._2.min(data2._2)) )  //用reduce做增量聚合

    minTempPerWindowStream.print("min temp")
    dataStream.print("input data")

    env.execute("ss")

  }
}

//class MyAssigner() extends  AssignerWithPeriodicWatermarks[SensorReading1]{
//  val bound = 60000
//  var maxTs = Long.MinValue
//
//  override def getCurrentWatermark: Watermark = new Watermark(maxTs - bound)
//
//  override def extractTimestamp(element:  SensorReading1, previousElementTimestamp: Long): Long =
//    {
//      maxTs = maxTs.max(element.timestamp * 1000)
//      element.timestamp * 1000
//    }
//
//}

class  MyAssigner extends AssignerWithPunctuatedWatermarks[SensorReading1]{
  override def checkAndGetNextWatermark(t: SensorReading1, l: Long): Watermark = new Watermark(l)

  override def extractTimestamp(t: SensorReading1, l: Long): Long = t.timestamp
}