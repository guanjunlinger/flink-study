package com.study.streaming.accumulator;

import com.study.streaming.datasource.CustomSourceFunction;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.accumulators.SimpleAccumulator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MyAccumulator implements SimpleAccumulator<Long> {

    private long total;

    @Override
    public void add(Long aLong) {
        total += aLong;
    }

    @Override
    public Long getLocalValue() {
        return total;
    }

    @Override
    public void resetLocal() {
        total = 0;
    }

    @Override
    public void merge(Accumulator<Long, Long> accumulator) {
        this.total += accumulator.getLocalValue();
    }

    @Override
    public Accumulator<Long, Long> clone() {
        MyAccumulator myAccumulator = new MyAccumulator();
        myAccumulator.total = this.total;
        return myAccumulator;
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.fromElements(1, 2, 4, 6, 9).map(new MyRichMapFunction()).print();

        JobExecutionResult jobExecutionResult = env.execute("MyAccumulator");
        Long result = jobExecutionResult.getAccumulatorResult("MyAccumulator");
        System.out.println("MyAccumulator:" + result);


    }
}
