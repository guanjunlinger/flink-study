package com.study;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;


public class WordCount {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        env.readTextFile("")
                .flatMap((String a, Collector<Tuple2<String, Integer>> b) -> {
                    String[] words = a.split(" ");
                    for (String word : words) {
                        b.collect(new Tuple2<>(word, 1));

                    }
                }).groupBy(value -> value.f0).reduce((tuple1, tuple2) ->
                new Tuple2<>(tuple1.f0, tuple1.f1 + tuple2.f1)).print();
    }
}
