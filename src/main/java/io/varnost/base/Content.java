package io.varnost.base;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Content {
    // Get specific element 1 lvl deep in JSON ObjectNode
    public static DataStream<String> getElement(DataStream<ObjectNode> stream, String element) {
        return stream.map(e -> e.get(element).asText());
    }

    // Filter by property
    public static DataStream<ObjectNode> filter(DataStream<ObjectNode> stream, String element, String match) {
        return stream.filter(node -> node.get(element).asText().equals(match));
    }

    // Group Events by JSON element 1 lvl deep in JSON ObjectNode
    public static DataStream<List<ObjectNode>> aggregate(DataStream<ObjectNode> stream, Time window, Time slide, Integer count) {
        return stream.map(e -> new Tuple2<>(Collections.singletonList(e), 1))
            .timeWindowAll(window, slide)
            .reduce( (a, b) ->
                new Tuple2<>(Stream.concat(a.f0.stream(), b.f0.stream()).collect(Collectors.toList()), a.f1 + b.f1)
            )
            .filter(e -> e.f1 > count)
            .map(e -> e.f0);
    }
}

