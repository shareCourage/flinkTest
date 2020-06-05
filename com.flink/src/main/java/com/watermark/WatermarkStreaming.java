package com.watermark;

import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

public class WatermarkStreaming {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(1000);

        OutputTag<Tuple2<String, Long>> outputTag = new OutputTag<Tuple2<String, Long>>("latest-date"){};

        DataStreamSource<String> dataStream = env.socketTextStream("localhost", 8888);

        SingleOutputStreamOperator<String> singleOutputStreamOperator = dataStream.map(new MapFunction<String, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(String line) throws Exception {
                String[] fields = line.split(",");
                return Tuple2.of(fields[0], Long.valueOf(fields[1]));
            }
        }).assignTimestampsAndWatermarks(new EventTimeExtractor())
                .keyBy(0)
                .timeWindow(Time.seconds(3))
//                .allowedLateness(Time.seconds(2)) //允许再次处理
                .sideOutputLateData(outputTag)//收集迟到未被处理的数据
                .process(new SumProcessWindowFunction());

        singleOutputStreamOperator.getSideOutput(outputTag).map(new MapFunction<Tuple2<String, Long>, String>() {
            @Override
            public String map(Tuple2<String, Long> stringLongTuple2) throws Exception {
                //正常情况下，可以生产进入kafka
                return "迟到的数据：" + stringLongTuple2.toString();
            }
        });

        DataStreamSink<String> dsSink = singleOutputStreamOperator
                                        .print()
                                        .setParallelism(1);

        env.execute(WatermarkStreaming.class.getName());
    }

    public static class EventTimeExtractor implements AssignerWithPeriodicWatermarks<Tuple2<String, Long>> {

        FastDateFormat dataFormat = FastDateFormat.getInstance("HH:mm:ss");
        private long currentMaxEventTime = 0L;
        private long maxOutOfOrderness = 10000;//允许的最大乱序时间

        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
            return new Watermark(currentMaxEventTime - maxOutOfOrderness);
        }

        @Override
        public long extractTimestamp(Tuple2<String, Long> element, long previousElementTimestamp) {
            long currentElementEventTime = element.f1;
            currentMaxEventTime = Math.max(currentElementEventTime, currentMaxEventTime);

            System.out.println("event = " + element
                    + "|" + dataFormat.format(element.f1) //eventtime
                    + "|" + dataFormat.format(currentMaxEventTime) //max event time
                    + "|" + dataFormat.format(getCurrentWatermark().getTimestamp()));//water mark time

            return currentElementEventTime;
        }
    }
    /**
     * IN 输入数据类型
     * OUT 输出数据类型
     * KEY，分组数据类型
     * W 窗口类型
     *
     */
    public static class SumProcessWindowFunction extends ProcessWindowFunction<
            Tuple2<String, Long>,
            String,
            Tuple,
            TimeWindow> {
        FastDateFormat dataFormat = FastDateFormat.getInstance("HH:mm:ss");


        @Override
        public void process(Tuple tuple,
                            Context context,
                            Iterable<Tuple2<String, Long>> elements,
                            Collector<String> collector) throws Exception {
            System.out.println("处理时间：" + dataFormat.format(context.currentProcessingTime()));
            System.out.println("window start time:" + dataFormat.format(context.window().getStart()));

            List<String> list = new ArrayList<String>();
            //可以重新进行排列
            for (Tuple2<String, Long> t: elements) {
                list.add(t.toString() + "|" +dataFormat.format(t.f1));
            }

            collector.collect(list.toString());

            System.out.println("window end time: " + dataFormat.format(context.window().getEnd()));
        }
    }
}
