package com.union;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

public class UnionTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Long> dataStream1 = env.addSource(new UnionSource()).setParallelism(1);
        DataStreamSource<Long> dataStream2 = env.addSource(new UnionSource()).setParallelism(1);

        DataStream<Long> union = dataStream1.union(dataStream2);
        SingleOutputStreamOperator<Long> map = union.map(new MapFunction<Long, Long>() {
            @Override
            public Long map(Long value) throws Exception {
                System.out.println("UnionTest map:" + value);
                return value;
            }
        });
//        SingleOutputStreamOperator<Long> sum = map.timeWindowAll(Time.seconds(2)).sum(0);
        map.print().setParallelism(1);
        env.execute(UnionTest.class.getSimpleName());
    }
}
