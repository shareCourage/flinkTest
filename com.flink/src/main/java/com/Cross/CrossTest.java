package com.Cross;

import org.apache.flink.api.common.functions.CrossFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.CrossOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;

import java.util.ArrayList;

public class CrossTest {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        ArrayList<Tuple2<Integer, String>> data1 = new ArrayList<>();
        data1.add(Tuple2.of(1, "bill"));
        data1.add(Tuple2.of(2, "jobs"));
        data1.add(Tuple2.of(3, "kobi"));

        ArrayList<Tuple2<Integer, String>> data2 = new ArrayList<>();
        data2.add(Tuple2.of(10, "US"));
        data2.add(Tuple2.of(20, "Koera"));
        data2.add(Tuple2.of(30, "HK"));

        DataSource<Tuple2<Integer, String>> s1 = env.fromCollection(data1);
        DataSource<Tuple2<Integer, String>> s2 = env.fromCollection(data2);

        CrossOperator<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple4<Integer, Integer, String, String>>
                with =
                s1.cross(s2).with(new CrossFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple4<Integer, Integer, String, String>>() {
                    @Override
                    public Tuple4<Integer, Integer, String, String> cross(Tuple2<Integer, String> t1, Tuple2<Integer, String> t2) throws Exception {
                        return Tuple4.of(t1.f0, t2.f0, t1.f1, t2.f1);
                    }
                });

        with.print();
    }
}
