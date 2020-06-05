
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class OrderETLStream {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> info1Stream = env.addSource(new FileSource(Constants.ORDER_INFO1_PATH));
        DataStreamSource<String> info2Stream = env.addSource(new FileSource(Constants.ORDER_INFO2_PATH));

        SingleOutputStreamOperator<OrderInfo1> map1 = info1Stream.map(line -> OrderInfo1.line2Info1(line));
        SingleOutputStreamOperator<OrderInfo2> map2 = info2Stream.map(line -> OrderInfo2.line2Info2(line));

        KeyedStream<OrderInfo1, Long> keyBy1 = map1.keyBy(orderInfo1 -> orderInfo1.getOrderId());

        KeyedStream<OrderInfo2, Long> keyBy2 = map2.keyBy(orderInfo2 -> orderInfo2.getOrderId());

        keyBy1.connect(keyBy2).flatMap(new EnrichmentFunction()).print();


        //./bin/flink run -c OrderETLStream /Users/PaulPan/IdeaProjects/com.flink/target/com.flink-1.0-SNAPSHOT.jar 127.0.0.1 9000

        env.execute("OrderETLStream");
    }
}
