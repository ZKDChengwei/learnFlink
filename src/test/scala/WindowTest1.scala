import com.atguigu.apitest.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object WindowTest1 {
  def main(args: Array[String]): Unit = {
    // 输出15内最小的温度值 滚动窗口
    // 1.获取环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
      // 设置并行度
    env.setParallelism(1)
    // 2.设置时间语义
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //3.设置源
    val stream = env.socketTextStream("localhost",7777)

    //4.数据转换
    val dataStream = stream.map(data =>{
      val dataArray = data.split(",")
      SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
    })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
        override def extractTimestamp(element: SensorReading): Long = {
          element.timestamp * 1000
        }
      })

    // 滚动窗口
    val minTempStream = dataStream.map(data =>(data.id,data.temperature))
      .keyBy(_._1)
      .timeWindow(Time.seconds(15))
      .reduce((result,data)=>(data._1,result._2.min(data._2)))
    dataStream.print("input data: ")
    minTempStream.print("min Temp: ")

    env.execute()
  }
}
