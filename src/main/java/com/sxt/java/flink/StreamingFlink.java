package com.sxt.java.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class StreamingFlink {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String node = "";
        Integer port = 0;
        if(parameterTool.has("node") && parameterTool.has("port")){
            node = parameterTool.get("node");
            port = Integer.valueOf(parameterTool.get("port"));
        }else {
            System.out.println("集群提交需要参数");
            System.exit(1);
        }

        DataStreamSource<String> streamSource = env.socketTextStream(node, port);
        SingleOutputStreamOperator<String> flatMap = streamSource.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String s, Collector<String> collector) throws Exception {
                for (String word : s.split(" ")) {
                    collector.collect(word);
                }
            }
        });
    }

}
