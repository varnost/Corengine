package io.varnost.content;

import io.varnost.base.RuleInterface;
import io.varnost.base.Content;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.datastream.DataStream;

public class FiveOhhh implements RuleInterface {
    @Override
    public DataStream<ObjectNode> logic(DataStream<ObjectNode> stream) {
        return Content.filter(stream, "response", "500");
    }
}
