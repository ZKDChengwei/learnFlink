import com.cw.entity.UserBehaviorJava;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.TimestampExtractor;

public class HotItemsJob {
    /*
需求说明：
-统计近1h内的热门商品，每5分钟更新一次
-热门度用浏览次数（pv）来衡量
 */
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 读取数据
        DataStreamSource<String> dataStreamSource = env.readTextFile("E:\\learn\\FlinkTutorial\\UserBehaviorAnalysis\\resource\\UserBehavior.csv");

        dataStreamSource.map(data -> {
            String[] dataArray = data.split(",");
            UserBehaviorJava userBehaviorJava = new UserBehaviorJava(Long.parseLong(dataArray[0].trim()), Long.parseLong(dataArray[1].trim()),
                    Integer.parseInt(dataArray[2].trim()), dataArray[3].trim(),
                    Long.parseLong(dataArray[4].trim()));
            return userBehaviorJava;
        })
                .assignTimestamps(new TimestampExtractor<UserBehaviorJava>() {
                    @Override
                    public long extractTimestamp(UserBehaviorJava element, long currentTimestamp) {
                        return 0;
                    }

                    @Override
                    public long extractWatermark(UserBehaviorJava element, long currentTimestamp) {
                        return 0;
                    }

                    @Override
                    public long getCurrentWatermark() {
                        return 0;
                    }
                });


        env.execute();

    }
}
