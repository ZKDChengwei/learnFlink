import com.chengwei.entity.SensorReading;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class ProcessFunctionT2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<String> stream = env.socketTextStream("localhost",7777);

        DataStream<SensorReading> dataStream = stream.map(data ->{
            String[] dataArray = data.split(",");
            return new SensorReading(dataArray[0].trim(),Long.parseLong(dataArray[1].trim()),Double.parseDouble(dataArray[2].trim()));
        })
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<SensorReading>(Time.seconds(1)) {
                    @Override
                    public long extractTimestamp(SensorReading element) {
                        return element.getTimestamp();
                    }
                });
        //需求：温度变化超过阈值发出警报！
        SingleOutputStreamOperator<String> process = dataStream.keyBy("id")
                .process(new tempChange(3.0));
        dataStream.print("input data: ");
        process.print("Temp change alert: ");

        env.execute();
    }

}
class tempChange extends KeyedProcessFunction<Tuple,SensorReading,String>{

    private Double threshold;
    private ValueState<Double> lastTemp;

    public tempChange(Double threshold) {
        this.threshold = threshold;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        lastTemp = getRuntimeContext().getState(new ValueStateDescriptor<Double>("last temp",Double.TYPE,0.0));
    }

    @Override
    public void processElement(SensorReading value, Context ctx, Collector<String> out) throws Exception {
        //取出上一次的温度
        double lastT = lastTemp.value();
        if (Math.abs(value.getTemperature() - lastT) > this.threshold){
            out.collect(value.getId() + ":" + "last temp=" + lastT + "  curr temp=" + value.getTemperature());
        }
        lastTemp.update(value.getTemperature());
    }

}
