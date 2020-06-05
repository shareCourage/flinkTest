import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;

public class EnrichmentFunction
        extends RichCoFlatMapFunction
        <OrderInfo1, OrderInfo2, Tuple2<OrderInfo1, OrderInfo2>> {

    private ValueState<OrderInfo1> orderInfo1ValueState;
    private ValueState<OrderInfo2> orderInfo2ValueState;
    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<OrderInfo1> orderInfo1ValueStateDescriptor = new ValueStateDescriptor<OrderInfo1>(
                "info1",
                OrderInfo1.class
        );

        ValueStateDescriptor<OrderInfo2> orderIinfo2ValueStateDescriptor = new ValueStateDescriptor<OrderInfo2>(
                "info2",
                OrderInfo2.class
        );

        orderInfo1ValueState = getRuntimeContext().getState(orderInfo1ValueStateDescriptor);
        orderInfo2ValueState = getRuntimeContext().getState(orderIinfo2ValueStateDescriptor);
    }

    @Override
    public void flatMap1(OrderInfo1 orderInfo1, Collector<Tuple2<OrderInfo1, OrderInfo2>> collector) throws Exception {
        OrderInfo2 value2 = orderInfo2ValueState.value();
        if (null != value2) {
            orderInfo2ValueState.clear();
            collector.collect(Tuple2.of(orderInfo1, value2));
        }
        else {
            orderInfo1ValueState.update(orderInfo1);
        }
    }

    @Override
    public void flatMap2(OrderInfo2 orderInfo2, Collector<Tuple2<OrderInfo1, OrderInfo2>> collector) throws Exception {
        OrderInfo1 value1 = orderInfo1ValueState.value();
        if (null != value1) {
            orderInfo1ValueState.clear();
            collector.collect(Tuple2.of(value1, orderInfo2));
        }
        else {
            orderInfo2ValueState.update(orderInfo2);
        }
    }
}
