package com.JoinTest;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.JoinOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;

public class JoinTest {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        ArrayList<Tuple2<Integer, String>> data1 = new ArrayList<>();
        data1.add(Tuple2.of(1, "bill"));
        data1.add(Tuple2.of(2, "jobs"));
        data1.add(Tuple2.of(3, "kobi"));

        ArrayList<Tuple2<Integer, String>> data2 = new ArrayList<>();
        data2.add(Tuple2.of(1, "US"));
        data2.add(Tuple2.of(2, "Koera"));
        data2.add(Tuple2.of(3, "HK"));

        DataSource<Tuple2<Integer, String>> tuple2DataSource1 = env.fromCollection(data1);
        DataSource<Tuple2<Integer, String>> tuple2DataSource2 = env.fromCollection(data2);


        JoinOperator.EquiJoin<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>>
                with =
                tuple2DataSource1
                        .join(tuple2DataSource2)
                        .where(0)
                        .equalTo(0)
                        .with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>>() {
                            @Override
                            public Tuple3<Integer, String, String> join(Tuple2<Integer, String> t1, Tuple2<Integer, String> t2) throws Exception {
                                return Tuple3.of(t1.f0, t1.f1, t2.f1);
                            }
                });
        with.print();
    }
}
