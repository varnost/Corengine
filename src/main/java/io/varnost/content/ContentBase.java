package io.varnost.content;

import java.util.Properties;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.api.common.serialization.SimpleStringSchema;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;

public class ContentBase {

  public static void main(String[] args) throws Exception {

    StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();

    Properties properties = new Properties();
    properties.setProperty("bootstrap.servers", "kf-service:9092");
    properties.setProperty("group.id", "varnost-content");
    DataStream<ObjectNode> logs = see
    	.addSource(new FlinkKafkaConsumer011<>("log-input",
                new JSONKeyValueDeserializationSchema(false), properties).setStartFromLatest());

    DataStream<String> names = logs
      .map(e -> e.get("value").get("response").asText());

    names
    .addSink(new FlinkKafkaProducer011<>("kf-service:9092", "log-output", new SimpleStringSchema()));

    see.execute();
  }
}