package com.study.streaming.sideoutput;

import com.study.streaming.datasource.CustomSourceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.OutputTag;


public class SideOutput {


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        OutputTag<Integer> high = new OutputTag<Integer>("high"){};
        OutputTag<Integer> low = new OutputTag<Integer>("low"){};

        DataStream<Integer> dataStream = env.addSource(new CustomSourceFunction()).process(new MyProcessFunction(high,low));

        DataStream<Integer> lowOutputStream =  ((SingleOutputStreamOperator<Integer>) dataStream).getSideOutput(low);
        DataStream<Integer> highOutputStream =  ((SingleOutputStreamOperator<Integer>) dataStream).getSideOutput(high);

        lowOutputStream.print();
        highOutputStream.print();

        env.execute("CustomSink");
    }
}
