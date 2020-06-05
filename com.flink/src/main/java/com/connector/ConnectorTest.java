package com.connector;

import com.union.UnionSource;
import com.union.UnionTest;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.util.Collector;

public class ConnectorTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Long> dataStream1 = env.addSource(new UnionSource(1)).setParallelism(1);
        DataStreamSource<Long> dataStream2 = env.addSource(new UnionSource(5)).setParallelism(1);

        ConnectedStreams<Long, Long> connect = dataStream1.connect(dataStream2);

//        SingleOutputStreamOperator<String> coMap = connect.map(new CoMapFunction<Long, Long, String>() {
//            @Override
//            public String map1(Long aLong) throws Exception {
//                System.out.println("map1...");
//                return "connect1:" + aLong;
//            }
//
//            @Override
//            public String map2(Long aLong) throws Exception {
//                System.out.println("map2...");
//                return "connect2 :" + aLong;
//            }
//        });
        SingleOutputStreamOperator<String> coFlatMap = connect.flatMap(new CoFlatMapFunction<Long, Long, String>() {
            @Override
            public void flatMap1(Long aLong, Collector<String> collector) throws Exception {
                System.out.println("flatMap1..." + aLong);
                collector.collect("flat1" + aLong);
            }

            @Override
            public void flatMap2(Long aLong, Collector<String> collector) throws Exception {
                System.out.println("flatMap2..." + aLong);
                collector.collect("flat2" + aLong);
            }
        });
//        coMap.print().setParallelism(1);
        coFlatMap.print().setParallelism(1);

        env.execute(ConnectorTest.class.getSimpleName());
    }
}
