package com.sxt.java.flink;

import org.apache.flink.api.common.accumulators.AccumulatorHelper;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

public class MyAccumulator {

    public static void main(String[] args) {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<String> dataSource = env.readTextFile("./data/words");
        FlatMapOperator<String, String> flatMapOperator = dataSource.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String s, Collector<String> collector) throws Exception {
                String[] line = s.split(" ");
                for (String word:line) {
                    collector.collect(word);
                }
            }
        });
        MapOperator<String, Tuple2<String, Integer>> mapOperator = flatMapOperator.map(new RichMapFunction<String, Tuple2<String, Integer>>() {

            IntCounter counter = new IntCounter();

            @Override
            public void open(Configuration parameters) throws Exception {
                counter.add(1);
                super.open(parameters);
            }

            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                return new Tuple2<>(s, 1);
            }
        });
        UnsortedGrouping<Tuple2<String, Integer>> groupBy = mapOperator.groupBy(0);
        AggregateOperator<Tuple2<String, Integer>> aggregateOperator = groupBy.sum(1);
        System.out.println();
    }
}
