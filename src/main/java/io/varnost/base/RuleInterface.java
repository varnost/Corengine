package io.varnost.base;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.datastream.DataStream;

public interface RuleInterface {
    DataStream<ObjectNode> logic(DataStream<ObjectNode> stream);
}
