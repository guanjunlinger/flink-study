package com.study.streaming.datasource;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class CustomSource {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Integer> dataStream = env.addSource(new CustomSourceFunction());
        dataStream.windowAll(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .sum("*");

        dataStream.print();
        env.execute("CustomSource");
    }
}
