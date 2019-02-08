package io.varnost.serialization;


import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;

public class LogMessageSchema implements DeserializationSchema<ObjectNode>, SerializationSchema<ObjectNode> {

    @Override
    public ObjectNode deserialize(byte[] bytes) throws IOException {
        return null;
    }

    @Override
    public byte[] serialize(ObjectNode myMessage) {
        return null;
    }

    @Override
    public TypeInformation<ObjectNode> getProducedType() {
        return TypeExtractor.getForClass(ObjectNode.class);
    }

    // Method to decide whether the element signals the end of the stream.
    // If true is returned the element won't be emitted.
    @Override
    public boolean isEndOfStream(ObjectNode myMessage) {
        return false;
    }
}