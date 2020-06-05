package com.Split;

import com.union.UnionSource;
import com.union.UnionTest;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;

public class SplitTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Long> dataStream1 = env.addSource(new UnionSource()).setParallelism(1);

        SplitStream<Long> split = dataStream1.split(new OutputSelector<Long>() {
            @Override
            public Iterable<String> select(Long aLong) {
                ArrayList<String> array = new ArrayList<>();
                if (aLong % 2 == 0) {
                    array.add("even");
                }
                else {
                    array.add("odd");
                }
                return array;
            }
        });
        DataStream<Long> even = split.select("even");
        DataStream<Long> odd = split.select("odd");
        DataStream<Long> more = split.select("even", "odd");

        even.print().setParallelism(1);
        env.execute(SplitTest.class.getSimpleName());

    }
}
