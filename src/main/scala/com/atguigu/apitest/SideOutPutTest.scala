package com.atguigu.apitest

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

object SideOutPutTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val stream = env.socketTextStream("localhost",7777)

    val dataStream = stream.map(data => {
      val dataArray = data.split(",")
      SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
    })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
        override def extractTimestamp(element: SensorReading): Long = element.timestamp * 1000
      })

    val processedStream = dataStream
      .process( new FreezingAlert())

//    dataStream.print("input data")
    processedStream.print("processed data")  //打印的是主流
    processedStream.getSideOutput(new OutputTag[String]("freezing alert")).print("alert data")

    env.execute("window test")

  }

}

class FreezingAlert() extends ProcessFunction[SensorReading,SensorReading]{ //主输出流的数据类型
  lazy val alertOutPut:OutputTag[String] = new OutputTag[String]("freezing alert")
  override def processElement(value: SensorReading, ctx: ProcessFunction[SensorReading, SensorReading]#Context, out: Collector[SensorReading]): Unit = {

    if( value.temperature < 32.0 ){
      ctx.output( alertOutPut, "freezing alert for " + value.id + value.temperature)
    } else {
      out.collect( value )
    }
  }
}