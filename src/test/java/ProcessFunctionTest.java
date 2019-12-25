//
//import com.chengwei.entity.SensorReading;
//import org.apache.flink.api.common.functions.RichFlatMapFunction;
//import org.apache.flink.api.common.state.ValueState;
//import org.apache.flink.api.common.state.ValueStateDescriptor;
//import org.apache.flink.api.java.tuple.Tuple;
//import org.apache.flink.configuration.Configuration;
//import org.apache.flink.streaming.api.TimeCharacteristic;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.datastream.DataStreamSource;
//import org.apache.flink.streaming.api.datastream.KeyedStream;
//import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//
//import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
//import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
//import org.apache.flink.streaming.api.windowing.time.Time;
//import org.apache.flink.util.Collector;
//
//
//
//public class ProcessFunctionTest {
//    public static void main(String[] args) throws Exception {
//        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);
//        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//
//        DataStreamSource<String> stream = env.socketTextStream("localhost",7777);
//
//        DataStream<SensorReading> dataStream = stream.map(data ->{
//           String[] dataArray = data.split(",");
//            SensorReading sensorReading = new SensorReading(dataArray[0], Long.parseLong(dataArray[1].trim()), Double.parseDouble(dataArray[2].trim()));
//            return sensorReading;
//        })
//                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<SensorReading>(Time.seconds(1)) {
//                    @Override
//                    public long extractTimestamp(SensorReading element) {
//                        return element.getTimestamp();
//                    }
//                });
//        //需求1：10s钟之内温度连续上升则报警
//        SingleOutputStreamOperator<String> processedStream = dataStream.keyBy("id")
//                .process(new TempIncreaseAlert());
//
//        //需求2：温度变化超过阈值发出警报！
//        SingleOutputStreamOperator<String> processedStream2 = dataStream.keyBy("id")
//                .process(new TempChangedAlert1(5.0));
//
//        //flatmap方式实现温度变化发出警报
//        SingleOutputStreamOperator<String> processedStream3 = dataStream.keyBy("id")
//                .flatMap(new TempChangedAlert2(5.0));
//
//        dataStream.print("input data: ");
////        processedStream.print("Temp increased alert: ");
////        processedStream2.print("Temp changed alert:");
//        processedStream3.print("Temp changed alert:");
//        env.execute();
//    }
//}
//
//class TempChangedAlert2 extends RichFlatMapFunction<SensorReading,String>{
//    Double threshold;
//    ValueState<Double> lastTemp;
//
//    public TempChangedAlert2(Double threshold) {
//        this.threshold = threshold;
//    }
//
//    @Override
//    public void open(Configuration parameters) throws Exception {
//        lastTemp = getRuntimeContext().getState(new ValueStateDescriptor<Double>("lastTemp",Double.TYPE,0.0));
//
//    }
//
//    @Override
//    public void flatMap(SensorReading value, Collector<String> out) throws Exception {
//        Double preTemp = lastTemp.value();
//        if(Math.abs(preTemp - value.getTemperature()) > this.threshold){
//            out.collect(value.getId()+" last temp="+preTemp + " curr Temp="+value.getTemperature());
//        }
//        lastTemp.update(value.getTemperature());
//    }
//}
//
//class TempChangedAlert1 extends KeyedProcessFunction<Tuple,SensorReading,String>{
//    //定义状态变量，记录上一次温度
//    private ValueState<Double> lastTemp;
//    private Double threshold;
//
//    public TempChangedAlert1(Double threshold) {
//        this.threshold = threshold;
//    }
//
//    @Override
//    public void open(Configuration parameters) throws Exception {
//        this.lastTemp = getRuntimeContext().getState(new ValueStateDescriptor<Double>("lastTemp",Double.TYPE,0.0));
//    }
//
//    @Override
//    public void processElement(SensorReading value, Context ctx, Collector<String> out) throws Exception {
//        // 取出上一次的温度值
//        Double preTemp = lastTemp.value();
//        // 如果差值超过阈值，则报警
//        if(Math.abs(value.getTemperature() - preTemp) > this.threshold){
//            out.collect(value.getId()+": last temp="+preTemp+"  curr Temp="+value.getTemperature());
//        }
//        lastTemp.update(value.getTemperature());
//    }
//}
//
//
//class TempIncreaseAlert extends KeyedProcessFunction<Tuple,SensorReading,String> {
//    ValueState<Double> lastTemp;
//    ValueState<Long> currTimer;
//    @Override
//    public void open(Configuration parameters) throws Exception {
//        super.open(parameters);
//        //定义状态变量，记录之前的温度
//        lastTemp = getRuntimeContext().getState(new ValueStateDescriptor<Double>("lastTemp", Double.TYPE,0.0));
//        //定义状态变量，记录定时器的时间戳
//        currTimer = getRuntimeContext().getState(new ValueStateDescriptor<Long>("currTimer",Long.TYPE,0L));
//    }
//
//    @Override
//    public void processElement(SensorReading value, Context ctx, Collector<String> out) throws Exception {
//        //取出上一个温度值
//        Double preTemp = lastTemp.value();
//        //更新状态
//        lastTemp.update(value.getTemperature());
//        //取出定时器的时间戳
//        Long currTs = currTimer.value();
//        //如果温度上升并且没有注册过定时器，则注册定时器
//        if(value.getTemperature() > preTemp && currTs == 0L){
//            Long timerTs = ctx.timerService().currentProcessingTime() + 10000L;
//            ctx.timerService().registerProcessingTimeTimer(timerTs);
//            currTimer.update(timerTs);
//        }
//        else if(value.getTemperature() < preTemp || preTemp == 0.0){
//            ctx.timerService().deleteProcessingTimeTimer(currTs);
//            currTimer.clear();
//        }
//    }
//
//    @Override
//    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
//        {
//            out.collect(ctx.getCurrentKey()+" 温度上升");
//            currTimer.clear();
//        }
//    }
//}