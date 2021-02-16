package com.study.streaming.watermark;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;


public class MyWatermarkStrategy implements WatermarkStrategy<MyEvent> {

    private long delay;
    private Boolean isPeriodic;

    public MyWatermarkStrategy(Long delay, Boolean isPeriodic) {
        this.delay = delay;
        this.isPeriodic = isPeriodic;
    }

    @Override
    public WatermarkGenerator<MyEvent> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
        if (isPeriodic) {
            return new PeriodicWatermarkGenerator(delay);
        }
        return new PunctuatedWatermarkGenerator();
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        List<MyEvent> list =new ArrayList<>(16);
        list.add(new MyEvent("gua",22,false));
        list.add(new MyEvent("jun",25,false));
        list.add(new MyEvent("liu",24,false));
        list.add(new MyEvent("cong",22,false));
        DataStream dataStream =env.fromCollection(list).assignTimestampsAndWatermarks(new MyWatermarkStrategy(3000L,true).withTimestampAssigner((event, timestamp) -> event.getTimestamp()));
        dataStream.print();
        env.execute();
    }

}
