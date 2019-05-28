package com.chongzi.stream.api;

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.util.Collector;

public class TestConnect {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env= StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Long> someStream = env.generateSequence(0,10);
        DataStream<String> otherStream = env.fromElements(WORDS);

        ConnectedStreams<Long, String> connectedStreams = someStream.connect(otherStream);

        DataStream<String> result=connectedStreams.flatMap(new CoFlatMapFunction<Long, String, String>() {

            @Override
            public void flatMap1(Long value, Collector<String> out) throws Exception {
                out.collect(value.toString());
            }

            @Override
            public void flatMap2(String value, Collector<String> out) {
                for (String word: value.split("\\W+")) {
                    out.collect(word);
                }
            }
        });

        result.print();

        env.execute();
    }

    public static final String[] WORDS = new String[] {
            "And thus the native hue of resolution",
            "Is sicklied o'er with the pale cast of thought;",
            "And enterprises of great pith and moment,",
            "With this regard, their currents turn awry,",
            "And lose the name of action.--Soft you now!",
            "The fair Ophelia!--Nymph, in thy orisons",
            "Be all my sins remember'd."
    };
}
