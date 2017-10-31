package com.example.onlineprediction.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class JsonDeserializer<T> implements Deserializer<T> {

    private ObjectMapper objectMapper = new ObjectMapper();
    private Class<T> tClass;

    @Override
    public void configure(Map<String, ?> props, boolean isKey) {
        tClass = (Class<T>) props.get("class");
    }

    @Override
    public T deserialize(String topic, byte[] bytes) {
        if(bytes == null)
            return null;

        T data;
        try {
            data = objectMapper.readValue(bytes, tClass);
        } catch(Exception e) {
            throw new SerializationException(e);
        }

        return data;
    }

    @Override
    public void close() {

    }
}