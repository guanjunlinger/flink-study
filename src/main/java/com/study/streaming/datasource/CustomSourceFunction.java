package com.study.streaming.datasource;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

public class CustomSourceFunction implements SourceFunction<Integer> {

    private Boolean isRunning = true;

    @Override
    public void run(SourceContext<Integer> sourceContext) {
        Random random = new Random();
        while (isRunning) {
            sourceContext.collect(random.nextInt(2));
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
