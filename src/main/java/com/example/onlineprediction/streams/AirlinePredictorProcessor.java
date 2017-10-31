package com.example.onlineprediction.streams;

import com.example.onlineprediction.ml.ModelBuilder;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.internals.MeteredKeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class AirlinePredictorProcessor extends AbstractProcessor<String, String> {

    private MeteredKeyValueStore<String, List<String>> flights;
    private static final Logger LOG = LoggerFactory.getLogger(AirlinePredictorProcessor.class);

    @Override
    public void init(ProcessorContext context) {
        super.init(context);
        flights = (MeteredKeyValueStore) context().getStateStore("flights");
        context().schedule(10000L);
    }

    @Override
    public void process(String airportId, String flightData) {
        List<String> flightList = this.flights.get(airportId);
        if(flightList == null)
            flightList = new ArrayList<>();

        LOG.debug("Adding key {}", airportId);
        flightList.add(flightData);
        this.flights.put(airportId, flightList);
    }

    @Override
    public void punctuate(long timestamp) {
        KeyValueIterator<String, List<String>> allFlights = flights.all();
        while (allFlights.hasNext()) {
            KeyValue<String, List<String>> kv = allFlights.next();
            List<String> flightList = kv.value;
            String airportCode = kv.key;
            LOG.debug("Found key {}", airportCode);
            if(flightList.size() >= 100) {
                try {
                    LOG.debug("Sending flight list {}", flightList);
                    byte[] serializedRegression = ModelBuilder.train(flightList);
                    context().forward(airportCode, serializedRegression);
                    LOG.info("updating model for {}", airportCode);
                    flightList.clear();
                    flights.put(airportCode, flightList);
                } catch(Exception e) {
                    LOG.error("Couldn't update online regression for {}", airportCode, e);
                }
            }
        }
        allFlights.close();
    }
}
