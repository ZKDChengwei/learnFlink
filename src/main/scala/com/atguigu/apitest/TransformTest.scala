package com.atguigu.apitest

import org.apache.flink.streaming.api.scala._

/**
  * @author
  * @create
  */
object TransformTest {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
//
//    val  streamFromFile = env.readTextFile("D:\\git\\FlinkTutorial\\src\\main\\resources\\sensor.txt")
//
//    val dataStream:DataStream[SensorReading] = streamFromFile.map(data =>{
//      val  dataArray = data.split(",")
//      SensorReading( dataArray(0).trim,dataArray(1).trim.toLong,dataArray(2).toDouble)
//
//    })
//
//    //1.
//      val aggStream = dataStream.keyBy("id")
////      .sum("temperature")
//      // 输出当前传感器最新的温度+10，而时间戳是上一次数据的时间+10
//      .reduce((x,y)=>SensorReading(x.id,x.timestamp+10,y.temperature+10))
//
//    //2.多流转换算子
//    val splitStream = dataStream.split(data=>{
//      if(data.temperature>40) Seq("high") else Seq("low")
//
//    })
//    val high = splitStream.select("high")
//    val low = splitStream.select("low")
//    val all = splitStream.select("high","low")
//
//    high.print("high")
//    low.print("low")
//    all.print("all")
//
//    val warning = high
//    dataStream.print()

    env.execute("transform test")
  }
}
