import com.chengwei.entity.SensorReading;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;


public class SideOutPutTest {
    public static void main(String[] args) throws Exception {
        //需求：冰点报警-将温度值低于32F 的温度输出到 side output。
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<String> stream = env.socketTextStream("localhost",7777);
        DataStream<SensorReading> dataStream = stream.map(data ->{
            String[] fields = data.split(",");
            return new SensorReading(fields[0].trim(),Long.parseLong(fields[1].trim()),Double.parseDouble(fields[2].trim()));
        });

        SingleOutputStreamOperator<SensorReading> processedStream = dataStream.process(new FreezAlert(32.0));


        processedStream.print("processed data(main stream):");
        DataStreamSink<String> freezing_alert = processedStream.getSideOutput(new OutputTag<String>("freezing alert", TypeInformation.of(String.class))).print("alert data:");

        env.execute("job");
    }
}

class FreezAlert extends ProcessFunction<SensorReading,SensorReading>{
    Double threshold;
    OutputTag<String> alertOutPut ;

    @Override
    public void open(Configuration parameters) throws Exception {
        alertOutPut = new OutputTag<String>("freezing alert",TypeInformation.of(String.class));
    }

    public FreezAlert(Double threshold) {
        this.threshold = threshold;
    }

    @Override
    public void processElement(SensorReading value, Context ctx, Collector<SensorReading> out) throws Exception {
        if(value.getTemperature() < this.threshold){
            ctx.output(alertOutPut,"freezing alert for "+value.getId()+" "+value.getTemperature());
//            ctx.output(alertOutPut,value);
        }
        else {
            out.collect(value);
        }
    }
}