package com.study.streaming.accumulator;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

public class MyRichMapFunction extends RichMapFunction<Integer, Integer> {

    private MyAccumulator myAccumulator = new MyAccumulator();


    public void open(Configuration parameters) {
        getRuntimeContext().addAccumulator("MyAccumulator", myAccumulator);
    }

    @Override
    public Integer map(Integer integer) {
        myAccumulator.add(1L);
        return integer;
    }
}
