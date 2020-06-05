package com.window;

import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.concurrent.TimeUnit;

public class Window1Source implements SourceFunction<String> {
    FastDateFormat dateFormat = FastDateFormat.getInstance("HH:mm:ss");
    @Override
    public void run(SourceContext<String> sourceContext) throws Exception {

        //10s的倍数时间点，发送时间
        String currTime = String.valueOf(System.currentTimeMillis());

        while (Integer.valueOf(currTime.substring(currTime.length() - 4)) > 100) {
            currTime = String.valueOf(System.currentTimeMillis());
            continue;
        }

        System.out.println("开始发送数据的时间：" + dateFormat.format(System.currentTimeMillis()));
        TimeUnit.SECONDS.sleep(3);

        sourceContext.collect("haoop:" + System.currentTimeMillis());
        sourceContext.collect("haoop:" + System.currentTimeMillis());
        TimeUnit.SECONDS.sleep(3);

        sourceContext.collect("haoop:" + System.currentTimeMillis());
        TimeUnit.SECONDS.sleep(3);
    }

    @Override
    public void cancel() {

    }
}
