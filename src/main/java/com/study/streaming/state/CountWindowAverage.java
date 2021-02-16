package com.study.streaming.state;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class CountWindowAverage extends RichFlatMapFunction<Tuple2<Long,Long>, Long> {

    private transient ValueState<Tuple2<Long, Long>> sum;

    @Override
    public void flatMap(Tuple2<Long,Long> tuple2, Collector<Long> collector) throws Exception {
        Tuple2<Long, Long> currentSum = sum.value();
        if (Objects.isNull(currentSum)) {
            sum.update(new Tuple2<>(0L, 0L));
            currentSum = sum.value();

        }
        currentSum.f0 += 1;
        currentSum.f1 += tuple2.f1;
        sum.update(currentSum);
        if (currentSum.f0 >= 2) {
            collector.collect(currentSum.f1 / currentSum.f0);
            sum.clear();
        }
    }

    @Override
    public void open(Configuration config){
        ValueStateDescriptor<Tuple2<Long, Long>> descriptor =
                new ValueStateDescriptor<>(
                        "average",
                        TypeInformation.of(new TypeHint<Tuple2<Long, Long>>() {
                        }));

        StateTtlConfig ttlConfig = StateTtlConfig
                .newBuilder(Time.seconds(50))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .build();
        descriptor.enableTimeToLive(ttlConfig);
        sum = getRuntimeContext().getState(descriptor);
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        List<Tuple2<Long,Long>> list =new ArrayList<>(16);
        list.add(Tuple2.of(1L,3L));
        list.add(Tuple2.of(1L,5L));
        list.add(Tuple2.of(1L,7L));
        list.add(Tuple2.of(1L,4L));
        list.add(Tuple2.of(1L,2L));
        env.fromCollection(list).keyBy(value->value.f0).flatMap(new CountWindowAverage()).print();

        env.execute("CountWindowAverage");
    }

}
