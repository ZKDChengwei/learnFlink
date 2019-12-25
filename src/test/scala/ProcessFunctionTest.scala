//import com.atguigu.apitest.SensorReading
//import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
//import org.apache.flink.streaming.api.TimeCharacteristic
//import org.apache.flink.streaming.api.functions.KeyedProcessFunction
//import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
//import org.apache.flink.streaming.api.scala._
//import org.apache.flink.streaming.api.windowing.time.Time
//import org.apache.flink.util.Collector
//
//object ProcessFunctionTest {
//  def main(args: Array[String]): Unit = {
//    //需求：1s钟之内温度连续上升则报警
//    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    env.setParallelism(1)
//    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
//
//    val stream = env.socketTextStream("localhost",7777)
//
//    val dataStream = stream.map(data=>{
//      val dataArray = data.split(",")
//      SensorReading(dataArray(0).trim,dataArray(1).trim.toLong,dataArray(2).trim.toDouble)
//    })
//        .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
//          override def extractTimestamp(element: SensorReading): Long = element.timestamp * 1000
//        })
//
//    val processedStream1 = dataStream.keyBy(_.id)
//      .process(new TempIncAlert())
//
//    dataStream.print("input data:")
//    processedStream1.print("Temp increased alert:")
//
//    env.execute()
//  }
//}
//class TempIncAlert() extends KeyedProcessFunction[String,SensorReading,String]{
//  // 定义一个状态 记录上一个温度
//  lazy val lastTemp:ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("lastTemp",classOf[Double]))
//  // 定义一个状态 记录定时器的时间戳
//  lazy val currentTimer:ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("currentTimer",classOf[Long]))
//
//  override def processElement(value: SensorReading, ctx: KeyedProcessFunction[String, SensorReading, String]#Context, out: Collector[String]): Unit = {
//    //取出上一个温度
//    val preTemp = lastTemp.value()
//    //更新温度
//    lastTemp.update(value.temperature)
//    //取出定时器的时间戳
//    val currTs = currentTimer.value()
//
//    //如果温度上升并且没有注册过定时器
//    if (value.temperature > preTemp && currTs == 0){
//      //注册定时器
//      val timerTs = ctx.timerService().currentProcessingTime() + 10000L
//      ctx.timerService().registerProcessingTimeTimer(timerTs)
//      currentTimer.update(timerTs)
//    }
//      //如果温度下降 或者是第一条数据
//    else if (value.temperature < preTemp || preTemp == 0.0){
//      //删除定时器并清空定时器时间戳状态
//      ctx.timerService().deleteProcessingTimeTimer(currTs)
//      currentTimer.clear()
//    }
//
//  }
//  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, SensorReading, String]#OnTimerContext, out: Collector[String]): Unit = {
//
//    out.collect(ctx.getCurrentKey +  "温度上升")
//    currentTimer.clear()
//  }
//
//}
//
