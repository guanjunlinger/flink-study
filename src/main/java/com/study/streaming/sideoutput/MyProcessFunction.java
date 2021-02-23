package com.study.streaming.sideoutput;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class MyProcessFunction extends ProcessFunction<Integer, Integer> {
    private OutputTag<Integer> low;
    private OutputTag<Integer> high;

    public MyProcessFunction(OutputTag high, OutputTag low) {
        this.high = high;
        this.low = low;
    }

    @Override
    public void processElement(Integer integer, Context context, Collector<Integer> collector){
        collector.collect(integer);
        if (integer > 0) {
            context.output(high, integer);
        } else {
            context.output(low, integer);
        }
    }
}
