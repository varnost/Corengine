package io.varnost.corengine;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.io.IOException;
import java.io.InputStream;
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
  private Properties prop;

  public LogStram(){
    try (InputStream input = LogStream.class.getClassLoader().getResourceAsStream("config.properties")) {

      prop = new Properties();

      if (input == null) {
        System.out.println("Sorry, unable to find config.properties");
        return;
      }

      //load a properties file from class path, inside static method
      prop.load(input);
    } catch (IOException ex) {
        ex.printStackTrace();
    }
  }
  public LogStream(String bootstrapServers, String groupId, String inputStram) {
    // Setup up execution environment
    see = StreamExecutionEnvironment.getExecutionEnvironment();
    see.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    see.enableCheckpointing(10000);
  }
  public DataStream<ObjectNode> openStream() {return openStream(Time.seconds(10));}
  public DataStream<ObjectNode> openStream(Time allowedLateness) {
    Properties properties = new Properties();
    properties.setProperty("bootstrap.servers", );
    properties.setProperty("group.id", prop.getProperty("kf.input.group"));

    SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");

    DataStream<ObjectNode> stream = see.addSource(new FlinkKafkaConsumer011<>(prop.getProperty("kf.input.stream"),
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
      .addSink(new FlinkKafkaProducer011<>(
        prop.getProperty("kf.host") + ":" + prop.getProperty("kf.port"),
        prop.getProperty("kf.output.group"),
        new SimpleStringSchema()));
    return see.execute();
  }

  public StreamExecutionEnvironment getSee() {
    return see;
  }
}
