package com.study;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


public class WordCount {

    public static void main(String[] args) {
        final ParameterTool params = ParameterTool.fromArgs(args);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(params);

        DataStream<String> text = env.readTextFile(params.get("input"))
                .flatMap((a,b)->{
                    String[] words = a.split(" ");
                    for(String word : words){
                         b.collect(new Tuple2(word,1));

                    } }).keyBy(value->value.f0));
    }
}
