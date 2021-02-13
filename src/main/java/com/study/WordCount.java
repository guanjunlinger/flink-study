package com.study;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;


public class WordCount {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<String> text = env.fromElements(
                "Who's there?",
                "I think I hear them. Stand, ho! Who's there?");
        text.flatMap((String a, Collector<Tuple2<String, Integer>> b) -> {
            String[] words = a.split(" ");
            for (String word : words) {
                b.collect(new Tuple2<>(word, 1));

            }
        }).groupBy(0).sum(1).print();
    }
}
