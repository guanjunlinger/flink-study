package com.study;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.FlatMapIterator;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * flink run -c com.study.WordCount  D:\gitRepository\flink-study\target\fink-study-1.0-SNAPSHOT.jar
 */
public class WordCount {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<String> text = env.fromElements(
                "Who's there?",
                "I think I hear them. Stand, ho! Who's there");
        text.flatMap(new MyFlatMapIterator()).groupBy(0).sum(1).print();
    }

    public static class MyFlatMapIterator extends FlatMapIterator<String, Tuple2<String, Integer>> {

        @Override
        public Iterator<Tuple2<String, Integer>> flatMap(String s) {
            String[] words = s.split(" ");
            List<Tuple2<String, Integer>> list = new ArrayList<>();
            for (String word : words) {
                list.add(new Tuple2<>(word, 1));
            }
            return list.iterator();
        }
    }
}
