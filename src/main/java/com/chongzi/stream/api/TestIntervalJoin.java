package com.chongzi.stream.api;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class TestIntervalJoin {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env= StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<Transcript> input1=env.fromElements(TRANSCRIPTS).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Transcript>() {
            @Override
            public long extractAscendingTimestamp(Transcript element) {
                return element.time;
            }
        });


        DataStream<Student> input2=env.fromElements(STUDENTS).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Student>() {
            @Override
            public long extractAscendingTimestamp(Student element) {
                return element.time;
            }
        });

        KeyedStream<Transcript,String> keyedStream=input1.keyBy(new KeySelector<Transcript, String>() {
            @Override
            public String getKey(Transcript value) throws Exception {
                return value.id;
            }
        });

        KeyedStream<Student,String> otherKeyedStream=input2.keyBy(new KeySelector<Student, String>() {
            @Override
            public String getKey(Student value) throws Exception {
                return value.id;
            }
        });

        //e1.timestamp + lowerBound <= e2.timestamp <= e1.timestamp + upperBound

        // key1 == key2 && leftTs - 2 < rightTs < leftTs + 2

        keyedStream.intervalJoin(otherKeyedStream)
                .between(Time.milliseconds(-2), Time.milliseconds(2))
                .upperBoundExclusive()
                .lowerBoundExclusive()
                .process(new ProcessJoinFunction<Transcript, Student, Tuple5<String,String,String,String,Integer>>() {

                    @Override
                    public void processElement(Transcript transcript, Student student, Context ctx, Collector<Tuple5<String, String, String, String, Integer>> out) throws Exception {
                        out.collect(Tuple5.of(transcript.id,transcript.name,student.class_,transcript.subject,transcript.score));
                    }

                }).print();

        env.execute();

    }

    public static final Transcript[] TRANSCRIPTS = new Transcript[] {
            new Transcript("1","张三","语文",100,System.currentTimeMillis()),
            new Transcript("2","李四","语文",78,System.currentTimeMillis()),
            new Transcript("3","王五","语文",99,System.currentTimeMillis()),
            new Transcript("4","赵六","语文",81,System.currentTimeMillis()),
            new Transcript("5","钱七","语文",59,System.currentTimeMillis()),
            new Transcript("6","马二","语文",97,System.currentTimeMillis())
    };

    public static final Student[] STUDENTS = new Student[] {
            new Student("1","张三","class1",System.currentTimeMillis()),
            new Student("2","李四","class1",System.currentTimeMillis()),
            new Student("3","王五","class1",System.currentTimeMillis()),
            new Student("4","赵六","class2",System.currentTimeMillis()),
            new Student("5","钱七","class2",System.currentTimeMillis()),
            new Student("6","马二","class2",System.currentTimeMillis())
    };

    private static class Transcript{
        private String id;
        private String name;
        private String subject;
        private int score;
        private long time;

        public Transcript(String id, String name, String subject, int score, long time) {
            this.id = id;
            this.name = name;
            this.subject = subject;
            this.score = score;
            this.time = time;
        }

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getSubject() {
            return subject;
        }

        public void setSubject(String subject) {
            this.subject = subject;
        }

        public int getScore() {
            return score;
        }

        public void setScore(int score) {
            this.score = score;
        }

        public long getTime() {
            return time;
        }

        public void setTime(long time) {
            this.time = time;
        }
    }

    private static class Student{
        private String id;
        private String name;
        private String class_;
        private long time;

        public Student(String id, String name, String class_, long time) {
            this.id = id;
            this.name = name;
            this.class_ = class_;
            this.time = time;
        }

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getClass_() {
            return class_;
        }

        public void setClass_(String class_) {
            this.class_ = class_;
        }

        public long getTime() {
            return time;
        }

        public void setTime(long time) {
            this.time = time;
        }
    }
}
