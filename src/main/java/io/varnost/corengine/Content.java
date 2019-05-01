package io.varnost.corengine;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.*;
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
        return filter(stream, element, Collections.singletonList(match));
    }

    public static DataStream<ObjectNode> filter(DataStream<ObjectNode> stream, String element, List<String> match) {
        return stream
            .filter(new FilterFunction<ObjectNode>() {
                @Override
                public boolean filter(ObjectNode node) throws Exception {
                    try {
                        return match.contains(node.get(element).asText());
                    }
                    catch (Exception e) {
                        return false;
                    }
                }
            });
    }
    // Group elements in a certain amount of time
    public static DataStream<List<ObjectNode>> aggregate(DataStream<ObjectNode> stream, Time window, Integer count) {
        return aggregate(stream, window, window, count);
    }
    public static DataStream<List<ObjectNode>> aggregate(DataStream<ObjectNode> stream, Time window, Time slide, Integer count) {
        return stream
            .map(new MapFunction<ObjectNode, List<ObjectNode>> () {
                @Override
                public List<ObjectNode> map(ObjectNode e) throws Exception {
                    return new ArrayList<>(Collections.singleton(e));
                }
            })
            .timeWindowAll(window, slide)
            .reduce(new ReduceFunction<List<ObjectNode>>() {
                @Override
                public List<ObjectNode> reduce(List<ObjectNode> a, List<ObjectNode> b) throws Exception {
                    return Stream.concat(a.stream(), b.stream()).collect(Collectors.toList());
                }
            })
            .filter(new FilterFunction<List<ObjectNode>>() {
                @Override
                public boolean filter(List<ObjectNode> e) throws Exception {
                    return e.size() > count;
                }
            })
//            .timeWindowAll(Time.milliseconds((window.toMilliseconds() * 2)))
//            .reduce(new ReduceFunction<List<ObjectNode>>() {
//                @Override
//                public List<ObjectNode> reduce(List<ObjectNode> a, List<ObjectNode> b) throws Exception {
//                    List<ObjectNode> combined = Stream.concat(a.stream(), b.stream()).collect(Collectors.toList());
//                    HashSet<Object> seen=new HashSet<>();
//                    combined.removeIf(e->!seen.add(e.get("uuid")));
//                    return combined;
//                }
//            })
        ;
    }
    public static DataStream<List<ObjectNode>> aggregateKeyed(DataStream<ObjectNode> stream, Time window, Integer count, String key) {
        return aggregateKeyed(stream, window, window, count, key);
    }
    public static DataStream<List<ObjectNode>> aggregateKeyed (DataStream<ObjectNode> stream, Time window, Time slide, Integer count, String key) {
        return stream
            .map(new MapFunction<ObjectNode, List<ObjectNode>> () {
                @Override
                public List<ObjectNode> map(ObjectNode e) throws Exception {
                    return new ArrayList<>(Collections.singleton(e));
                }
            })
            .keyBy(e -> e.get(0).get(key))
            .timeWindow(window, slide)
            .reduce(new ReduceFunction<List<ObjectNode>>() {
                @Override
                public List<ObjectNode> reduce(List<ObjectNode> a, List<ObjectNode> b) throws Exception {
                    return Stream.concat(a.stream(), b.stream()).collect(Collectors.toList());
                }
            })
            .filter(new FilterFunction<List<ObjectNode>>() {
                @Override
                public boolean filter(List<ObjectNode> e) throws Exception {
                    return e.size() > count;
                }
            })
//            .keyBy(e -> e.get(0).get(key))
//            .timeWindow(Time.milliseconds((window.toMilliseconds() * 2)))
//            .reduce(new ReduceFunction<List<ObjectNode>>() {
//                @Override
//                public List<ObjectNode> reduce(List<ObjectNode> a, List<ObjectNode> b) throws Exception {
//                    List<ObjectNode> combined = Stream.concat(a.stream(), b.stream()).collect(Collectors.toList());
//                    HashSet<Object> seen=new HashSet<>();
//                    combined.removeIf(e->!seen.add(e.get("uuid")));
//                    return combined;
//                }
//            })
            ;
    }

    public static DataStream<List<ObjectNode>> suppressWindowResult(DataStream<List<ObjectNode>> stream) {
        return null;
    }

    // Given a list of UUIDs, create an alert
    public static DataStream<Alert> createAlert(String n, String sd, String d, DataStream<List<String>> uuids) {
        return uuids.map(new MapFunction<List<String>, Alert>() {
            @Override
            public Alert map(List<String> uuids) throws Exception {
                Alert alert = new Alert(n, sd, d, uuids);
                alert.createESQuery();
                return alert;
            }
        });
    }
}

