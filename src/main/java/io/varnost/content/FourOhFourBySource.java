package io.varnost.content;

import io.varnost.base.Alert;
import io.varnost.base.Content;
import io.varnost.base.RuleInterface;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

public class FourOhFourBySource implements RuleInterface {
    @Override
    public DataStream<Alert> logic(DataStream<ObjectNode> stream) {
        DataStream<ObjectNode> filtered = Content.filter(stream, "response", "404");

        DataStream<List<ObjectNode>> aggregated = Content.aggregateKeyed(filtered, Time.minutes(1), 10, "source");

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
        "TwoHuny",
        "Got some traffic!",
        "Triggers when more than 10 events come in with response code 200 in 1 minute.\n" +
                "This is a sliding window by 1 second",
            uuids
        );
    }
}