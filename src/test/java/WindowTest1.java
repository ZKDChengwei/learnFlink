//import com.chengwei.entity.SensorReading;
//import org.apache.flink.streaming.api.TimeCharacteristic;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
//import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
//import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
//import org.apache.flink.streaming.api.watermark.Watermark;
//import org.apache.flink.streaming.api.windowing.time.Time;
//
//import javax.annotation.Nullable;
//import java.util.HashMap;
//import java.util.Map;
//
//public class WindowTest1 {
//    public static void main(String[] args) throws Exception {
//        // 输出15内最小的温度值 滚动窗口,滑动窗口
//        // 1.获取环境
//        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        // 设置并行度
//        env.setParallelism(1);
//        // 2.设置时间语义
//        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//
//        //3.数据读取
//        DataStream<String> stream = env.socketTextStream("localhost",7777);
//
//
//        //4.数据转换
//
//        // 方式1：采用BoundedOutOfOrdernessTimestampExtractor指定时间戳和水位线
//        DataStream<SensorReading> dataStream = stream.map((String data) ->{
//            String[] fields = data.split(",");
//            return new SensorReading(fields[0].trim(),Long.parseLong(fields[1].trim()),Double.parseDouble(fields[2].trim()));
//        })
//                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<SensorReading>(Time.seconds(1)) {
//                    @Override
//                    public long extractTimestamp(SensorReading element) {
//                        return element.getTimestamp() * 1000;
//                    }
//                });
//        // 方式2：自定义Assigner 实现AssignerWithPeriodicWatermarks:
////        DataStream<SensorReading> dataStream = stream.map((String data) ->{
////            String[] fields = data.split(",");
////            return new SensorReading(fields[0].trim(),Long.parseLong(fields[1].trim()),Double.parseDouble(fields[2].trim()));
////        })
////                .assignTimestampsAndWatermarks(new TestAssigner1());
//
//        // 方式3：自定义Assigner 实现AssignerWithPeriodicWatermarks:
////        DataStream<SensorReading> dataStream = stream.map((String data) ->{
////            String[] fields = data.split(",");
////            return new SensorReading(fields[0].trim(),Long.parseLong(fields[1].trim()),Double.parseDouble(fields[2].trim()));
////        })
////                .assignTimestampsAndWatermarks(new TestAssigner2());
//
//
//        // 滚动窗口：
////        DataStream<SensorReading> minTempStream = dataStream.keyBy("id")
////                .timeWindow(Time.seconds(15))
////                .reduce((SensorReading res, SensorReading data)->{
////                    if (res.getTemperature() > data.getTemperature()){
////                        return data;
////                    } else return res;
////                });
//
//        // 滑动窗口：
//        DataStream<SensorReading> minTempStream = dataStream.keyBy("id")
//                .timeWindow(Time.seconds(10),Time.seconds(5))
//                .reduce((SensorReading res, SensorReading data)->{
//                    if (res.getTemperature() > data.getTemperature()){
//                        return data;
//                    } else return res;
//                });
//
//
//        dataStream.print("input data: ");
//        minTempStream.print("min Temp: ");
//
//        env.execute("WindowTest1 java");
//
//    }
//
//
//}
//
//class TestAssigner1 implements AssignerWithPeriodicWatermarks<SensorReading> {
//    private int bound = 60000;
//    private Long maxTs = Long.MIN_VALUE;
//    @Nullable
//    @Override
//    public Watermark getCurrentWatermark() {
//        return new Watermark(maxTs - bound);
//    }
//
//    @Override
//    public long extractTimestamp(SensorReading element, long previousElementTimestamp) {
//        maxTs = Math.max(element.getTimestamp()*1000,maxTs);
//        return element.getTimestamp() * 1000;
//    }
//}
//
//class TestAssigner2 implements AssignerWithPunctuatedWatermarks<SensorReading>{
//    @Nullable
//    @Override
//    public Watermark checkAndGetNextWatermark(SensorReading lastElement, long extractedTimestamp) {
//        return new Watermark(extractedTimestamp);
//    }
//
//    @Override
//    public long extractTimestamp(SensorReading element, long previousElementTimestamp) {
//        return element.getTimestamp()*1000;
//    }
//}
//
