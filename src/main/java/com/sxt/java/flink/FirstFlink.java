package com.sxt.java.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import scala.Int;

public class FirstFlink {

    public static void main(String[] args) {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<String> dataSource = env.readTextFile("./data/words");
        FlatMapOperator<String, String> flatMapOperator = dataSource.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String s, Collector<String> collector) throws Exception {
                for (String word : s.split(" ")) {
                    collector.collect(word);
                }
            }
        });
        MapOperator<String, Tuple2<String, Integer>> mapOperator = flatMapOperator.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                return new Tuple2<>(s, 1);
            }
        });
        UnsortedGrouping<Tuple2<String, Integer>> unsortedGrouping = mapOperator.groupBy(0);
        AggregateOperator<Tuple2<String, Integer>> aggregateOperator = unsortedGrouping.sum(1).setParallelism(1);
        SortPartitionOperator<Tuple2<String, Integer>> sortPartitionOperator = aggregateOperator.sortPartition(1, Order.DESCENDING);
        try {
            sortPartitionOperator.print();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
