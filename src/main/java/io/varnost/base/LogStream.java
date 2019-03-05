package io.varnost.base;

import java.util.*;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.api.common.serialization.SimpleStringSchema;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;

public class LogStream {

  private StreamExecutionEnvironment see;

  public LogStream() {
    // Setup up execution environment
    see = StreamExecutionEnvironment.getExecutionEnvironment();
  }

  public DataStream<ObjectNode> openStream() {
    Properties properties = new Properties();
    properties.setProperty("bootstrap.servers", "kf-service:9092");
    properties.setProperty("group.id", "varnost-content");
    return  see.addSource(new FlinkKafkaConsumer011<>("log-input",
            new JSONKeyValueDeserializationSchema(false), properties).setStartFromLatest())
            .map((MapFunction<ObjectNode, ObjectNode>) jsonNodes -> (ObjectNode) jsonNodes.get("value"));
  }

  public JobExecutionResult closeStream(DataStream<ObjectNode> alerts) throws Exception {
    // Convert to string and sink to Kafka
    alerts.map((MapFunction<ObjectNode, String>) ObjectNode::toString)
            .addSink(new FlinkKafkaProducer011<>("kf-service:9092", "log-output", new SimpleStringSchema()));
    return see.execute();
  }

  public StreamExecutionEnvironment getSee() {
    return see;
  }
}
