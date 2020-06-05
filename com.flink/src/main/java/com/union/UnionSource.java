package com.union;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class UnionSource implements SourceFunction<Long> {

    public UnionSource(Integer s) {
        this.sleepTime = s;
    }

    public UnionSource(){

    }

    private Integer sleepTime = 1;

    private Boolean isRunning = true;
    private Long count = 0L;
    @Override
    public void run(SourceContext<Long> sourceContext) throws Exception {
        System.out.println("run......");
        while (isRunning) {
            sourceContext.collect(count);
            Thread.sleep(1000 * this.sleepTime);
            count ++;
        }
    }

    @Override
    public void cancel() {
        System.out.println("cancel......");
        isRunning = false;
    }
}
