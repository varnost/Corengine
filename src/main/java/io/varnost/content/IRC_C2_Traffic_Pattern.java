package io.varnost.content;

import io.varnost.base.Alert;
import io.varnost.base.Content;
import io.varnost.base.RuleInterface;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

public class IRC_C2_Traffic_Pattern implements RuleInterface {
    @Override
    public DataStream<Alert> logic(DataStream<ObjectNode> stream) {
        DataStream<ObjectNode> f1 = Content.filter(stream, "type", "firewall");
        DataStream<ObjectNode> f2 = Content.filter(f1, "dest_port", Arrays.asList(
                "6660",
                "6661",
                "6662",
                "6663",
                "6664",
                "6665",
                "6666",
                "6667",
                "6668",
                "6669",
                "7000"
        ));


        DataStream<List<ObjectNode>> aggregated = Content.aggregateKeyed(f2, Time.minutes(1), Time.seconds(1), 2, "src_ip");

        DataStream<List<String>> uuids = aggregated
            .map(new MapFunction<List<ObjectNode>, List<String>>() {
                @Override
                public List<String> map(List<ObjectNode> objectNode) throws Exception {
                    final ObjectMapper mapper = new ObjectMapper();
                    return objectNode
                        .stream()
                        .map(new Function<ObjectNode, String>() {
                            @Override
                            public String apply(ObjectNode omap) {
                                return omap.get("uuid").asText();
                            }
                        })
                        .collect(Collectors.toList());
                }
            });

        return Content.createAlert(
        "IRC C2 Traffic Pattern",
        "IRC Traffic looks like C2",
        "This rule is designed to detect IRC traffic that matches C2 communication patterns. IRC is a protocol \n" +
            "that has been commonly known to be used in C2 communication.  Reoccurring IRC traffic to external \n" +
            "hosts at regular intervals can be an indication of C2 traffic. \n" +
            "\n" +
            "\n" +
            "   24 Firewall events\n" +
            "   Where the destination port is one of [6660-6669,7000]\n" +
            "   From 1 internal source to 1 external destination\n" +
            "   Where the events are aggregated in unique minutes\n" +
            "   Within 2 hours",
            uuids
        );
    }
}