package com.example.onlineprediction.streams;

import com.example.onlineprediction.data.DataLoader;
import com.example.onlineprediction.ml.ModelBuilder;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class DataProducer {

    private static final Logger LOG = LoggerFactory.getLogger(DataProducer.class);
    private static final String PATH = "src/main/resources/flights.csv";

    public static void main(String[] args) throws InterruptedException, IOException {
        if(args.length > 0) {
            LOG.info("Running in populate GlobalKTable Mode");
            populateGlobalKTable();
        } else {
            LOG.info("Sending simulated for updating model");
            List<String> sampleFiles = Arrays.asList("june_2017.csv", "july_2017.csv", "august_2017.csv");
            for(String sampleFile : sampleFiles) {
                sendRawData(sampleFile);
                Thread.sleep(30000);
            }
        }
    }

    private static void sendRawData(String sampleFile) throws IOException, InterruptedException {
        LOG.info("send some raw data");
        Map<String, List<String>> dataToLoad = DataLoader.getFlightDataByAirport("src/main/resources/"+ sampleFile);
        Producer<String, String> producer = getDataProducer();
        int counter = 0;

        for(Map.Entry<String, List<String>> entry : dataToLoad.entrySet()) {
            String key = entry.getKey();
            for(String flight : entry.getValue()) {
                ProducerRecord<String, String> rawDataRecord = new ProducerRecord<String, String>("raw-airline-data", key, flight);
                ProducerRecord<String, String> incomingMLData = new ProducerRecord<String, String>("ml-data-input", key, flight);
                producer.send(rawDataRecord);
                producer.send(incomingMLData);
                counter++;
                if(counter>0 && counter % 10 == 0){
                    Thread.sleep(10000);
                }
            }
        }

        LOG.info("Sent {} numbers of raw data feed.", counter);
        producer.close();
    }

    private static void populateGlobalKTable() {
        LOG.info("Building the model.");

        Map<String, List<String>> model = ModelBuilder.train(PATH);
    }

    public static Producer<String,String> getDataProducer() {
        Properties props = getProps("org.apache.kafka.common.serialization.StringSerialization");
        return new KafkaProducer<String, String>(props);
    }

    private static Properties getProps(String valueSerialization) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerialization");
        props.put("value.serialization", valueSerialization);
        return props;
    }
}
