package io.varnost.base;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
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
    see.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
  }
  public DataStream<ObjectNode> openStream() {return openStream(Time.seconds(10));}
  public DataStream<ObjectNode> openStream(Time allowedLateness) {
    Properties properties = new Properties();
    properties.setProperty("bootstrap.servers", "kf-service:9092");
    properties.setProperty("group.id", "varnost-content");

    SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");

    DataStream<ObjectNode> stream = see.addSource(new FlinkKafkaConsumer011<>("log-input",
            new JSONKeyValueDeserializationSchema(false), properties).setStartFromLatest());
    return stream
        .map((MapFunction<ObjectNode, ObjectNode>) jsonNodes -> (ObjectNode) jsonNodes.get("value"))
        .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<ObjectNode>(allowedLateness) {
          @Override
          public long extractTimestamp(ObjectNode node) {
            try {
              return f.parse(node.get("@timestamp").asText()).getTime();
            } catch (ParseException e) {
              e.printStackTrace();
              return 0;
            }
          }
        });
  }

  public JobExecutionResult closeStream(DataStream<Alert> alerts) throws Exception {
    // Convert to string and sink to Kafka
    alerts
      .map(new MapFunction<Alert, String>() {
        @Override
        public String map(Alert alert) throws Exception {
          return new ObjectMapper().writer().withDefaultPrettyPrinter().writeValueAsString(alert);
        }
      })
      .addSink(new FlinkKafkaProducer011<>("kf-service:9092", "log-output", new SimpleStringSchema()));
    return see.execute();
  }

  public StreamExecutionEnvironment getSee() {
    return see;
  }
}
