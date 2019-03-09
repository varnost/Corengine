package io.varnost.base;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Content {
    // Get specific element 1 lvl deep in JSON ObjectNode
    public static DataStream<String> getElement(DataStream<ObjectNode> stream, String element) {
        return stream
            .map(new MapFunction<ObjectNode, String>() {
                @Override
                public String map(ObjectNode e) throws Exception {
                    return e.get(element).asText();
                }
            });
    }

    // Filter by property
    public static DataStream<ObjectNode> filter(DataStream<ObjectNode> stream, String element, String match) {
        return stream
            .filter(new FilterFunction<ObjectNode>() {
                @Override
                public boolean filter(ObjectNode node) throws Exception {
                    return node.get(element).asText().equals(match);
                }
            });
    }

    // Group Events by JSON element 1 lvl deep in JSON ObjectNode
    public static DataStream<List<ObjectNode>> aggregate(DataStream<ObjectNode> stream, Time window, Time slide, Integer count) {
        return stream
            .map(new MapFunction<ObjectNode, Tuple2<List<ObjectNode>, Integer>>() {
                @Override
                public Tuple2<List<ObjectNode>, Integer> map(ObjectNode e) throws Exception {
                    return new Tuple2<>(Collections.singletonList(e), 1);
                }
            })
            .timeWindowAll(window, slide)
            .reduce(new ReduceFunction<Tuple2<List<ObjectNode>, Integer>>() {
                @Override
                public Tuple2<List<ObjectNode>, Integer> reduce(Tuple2<List<ObjectNode>, Integer> a, Tuple2<List<ObjectNode>, Integer> b) throws Exception {
                    return new Tuple2<>(Stream.concat(a.f0.stream(), b.f0.stream()).collect(Collectors.toList()), a.f1 + b.f1);
                }
            })
            .filter(new FilterFunction<Tuple2<List<ObjectNode>, Integer>>() {
                @Override
                public boolean filter(Tuple2<List<ObjectNode>, Integer> e) throws Exception {
                    return e.f1 > count;
                }
            })
            .timeWindowAll(Time.milliseconds((window.toMilliseconds() * 2)))
            .reduce(new ReduceFunction<Tuple2<List<ObjectNode>, Integer>>() {
                @Override
                public Tuple2<List<ObjectNode>, Integer> reduce(Tuple2<List<ObjectNode>, Integer> a, Tuple2<List<ObjectNode>, Integer> b) throws Exception {
                    Tuple2<List<ObjectNode>, Integer> combined = new Tuple2<>(Stream.concat(a.f0.stream(), b.f0.stream()).collect(Collectors.toList()), a.f1 + b.f1);
                    HashSet<Object> seen=new HashSet<>();
                    combined.f0.removeIf(e->!seen.add(e.get("uuid")));
                    return combined;
                }
            })
            .map(new MapFunction<Tuple2<List<ObjectNode>, Integer>, List<ObjectNode>>() {
                @Override
                public List<ObjectNode> map(Tuple2<List<ObjectNode>, Integer> e) throws Exception {
                    return e.f0;
                }
            });
    }

    // Given a list of UUIDs, create an alert
    public static DataStream<Alert> createAlert(String n, String sd, String d, DataStream<List<String>> uuids) {
        return uuids.map(new MapFunction<List<String>, Alert>() {
            @Override
            public Alert map(List<String> uuids) throws Exception {
                return new Alert(n, sd, d, uuids);
            }
        });
    }
}

