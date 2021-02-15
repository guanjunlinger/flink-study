package com.study.streaming.datasink;


import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

public class MySinkFunction extends RichSinkFunction<Integer> {


    public void configure(Configuration configuration) {
        System.out.println("开始配置自定义输出流");

    }

    public void open(Configuration parameters) {
        System.out.println("开始打开自定义输出流");
    }

    public void close() {
        System.out.println("开始关闭输出流");
    }

    public void invoke(Integer value, SinkFunction.Context context) {
        System.out.println("开始写入:" + value);
    }
}
