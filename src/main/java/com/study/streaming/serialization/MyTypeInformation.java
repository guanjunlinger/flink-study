package com.study.streaming.serialization;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;

public class MyTypeInformation {


    public static void main(String[] args) {

        TypeInformation<String> info = TypeInformation.of(String.class);
        System.out.println(info);

        TypeInformation<Tuple2<String, Double>> tuple2TypeInformation = TypeInformation.of(new TypeHint<Tuple2<String, Double>>(){});

        System.out.println(tuple2TypeInformation);
    }
}
