package com.study.streaming.datasink;

import com.study.streaming.datasource.CustomSourceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class CustomSink {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Integer> dataStream = env.addSource(new CustomSourceFunction()).windowAll(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .sum("*");
        dataStream.addSink(new MySinkFunction());
        env.execute("CustomSink");
    }
}