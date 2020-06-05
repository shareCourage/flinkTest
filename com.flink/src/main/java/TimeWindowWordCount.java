import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;

public class TimeWindowWordCount {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<String> dStream = env.socketTextStream("localhost", 9898);

        dStream.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
        @Override
        public void flatMap(String line, Collector<Tuple2<String, Integer>> out) throws Exception {

            String[] fields = line.split(",");
            for (String word: fields) {
                out.collect(Tuple2.of(word, 1));
            }
        }
        }).assignTimestampsAndWatermarks(new EventTimeExtractor())
                .keyBy(0)
                .timeWindow(Time.seconds(10), Time.seconds(5))
                .process(new SumProcessWindow())
                .print()
                .setParallelism(1);
    //每隔5s，计算最近10s出现的单词计数
        env.execute("TimeWindowWordCount");

    }

    public static class EventTimeExtractor implements AssignerWithPeriodicWatermarks<Tuple2<String, Integer>> {


        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
            return new Watermark(System.currentTimeMillis());
        }

        @Override
        public long extractTimestamp(Tuple2<String, Integer> stringIntegerTuple2, long l) {
            return stringIntegerTuple2.f1;
        }
    }
    /**
     * IN 输入数据类型
     * OUT 输出数据类型
     * KEY，分组数据类型
     * W 窗口类型
     *
    */
    public static class SumProcessWindow extends ProcessWindowFunction<
            Tuple2<String, Integer>,
            Tuple2<String, Integer>,
            Tuple,
            TimeWindow> {
//        FastDateFormat.getDateInstance("HH:mm:ss");
        @Override
        public void process(Tuple tuple,
                            Context context,
                            Iterable<Tuple2<String, Integer>> elements,
                            Collector<Tuple2<String, Integer>> out) throws Exception {
//            System.out.println("当前系统时间：" + );
            int count = 0;
            for (Tuple2<String, Integer> ele: elements) {
                count ++;
            }
            out.collect(Tuple2.of(tuple.getField(0), count));
        }
    }
}
