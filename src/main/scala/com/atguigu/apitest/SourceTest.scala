package com.atguigu.apitest

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

import scala.util.Random


/**
  * @author
  * @create
  */

//温度传感器读数样例类
case class SensorReading( id:String, timestamp:Long, temperature:Double )

object SourceTest {
  def main(args: Array[String]): Unit = {

    val env=StreamExecutionEnvironment.getExecutionEnvironment

    // 1.从自定义的集合中读取数据
    val stream1=env.fromCollection( List(
      SensorReading("s1",11111111,35),
      SensorReading("s2",11111112,36.5),
      SensorReading("s3",11111113,36),
      SensorReading("s4",11111114,37)
    ))

    stream1.print()
//    env.fromElements(1,2.0,"string").print()
    // 2.从文件中读取数据
//    val stream2=env.readTextFile("D:\\git\\FlinkTutorial\\src\\main\\resources\\sensor.txt")

    // 3.从kafka中读取数据
//    val properties=new Properties()
//    properties.setProperty("bootstrap","localhost:9092")
//    val stream3=env.addSource(new FlinkKafkaConsumer011[String]("sensor",new SimpleStringSchema(),properties))
//    stream3.print("stream3")
//
//    stream2.print("stream1").setParallelism(1)

//    val stream4=env.addSource(new SensorSource)
//    stream4.print("stream4").setParallelism(1)

    env.execute("source test")
  }
}

class SensorSource() extends SourceFunction[SensorReading]{

  //定义一个flag，表示数据源是否正常运行
  var running:Boolean =true

  //取消数据源的生成
  override def cancel():Unit={
    running=false
  }

  //正常生成数据
  override def run(ctx: SourceFunction.SourceContext[SensorReading]): Unit = {
    //初始化一个随机数发生器
    val rand=new Random()

    //初始化定义一组传感器温度数据
    var curTemp=1.to(10).map(
      i=>("sensor_"+i,60+rand.nextGaussian()*20)
    )

    // 用无限循环，产生数据流
    while (running){
      // 更新数据
      curTemp=curTemp.map(
        t=>(t._1,t._2+rand.nextGaussian())
      )
      //获取当前时间戳
      val curTime=System.currentTimeMillis()
      curTemp.foreach(
        t=>ctx.collect(SensorReading(t._1,curTime,t._2))
      )
      //设置时间间隔
      Thread.sleep(500)
    }
  }
}
