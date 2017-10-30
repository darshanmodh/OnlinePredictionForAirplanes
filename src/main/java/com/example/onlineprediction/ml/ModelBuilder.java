package com.example.onlineprediction.ml;

import com.example.onlineprediction.data.DataLoader;
import org.apache.mahout.classifier.sgd.OnlineLogisticRegression;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.SecureRandom;
import java.util.*;

public class ModelBuilder {

    private static final int NUM_EPOCS = 20;
    private static final double PERCENTAGE_OF_DATASET_TO_USE = 0.50;
    private static final double TRAINING_PERCENTAGE = 0.40;
    private static final Logger LOG = LoggerFactory.getLogger(ModelBuilder.class);
    private static final String PATH = "src/main/resources/flights.csv";

    public static void main(String[] args) throws IOException {
        LOG.info("Training Now. Called from command line.");
        train(PATH);
    }

    public static Map<String, List<OnlineLogisticRegression>> train(String path) throws IOException {
        final Map<String, List<OnlineLogisticRegression>> regressionMap = new HashMap<>();

        final Map<String, List<String>> data = DataLoader.getFlightDataByAirport(path);

        final Map<String, List<String>> sample = getRandomSampling(data);

        final List<FlightData> flightData = new ArrayList<>();

        // TODO: train the data

        return regressionMap;
    }

    private static Map<String,List<String>> getRandomSampling(Map<String, List<String>> allData) {
        Map<String, List<String>> sample = new HashMap<>();
        SecureRandom random = new SecureRandom();
        for(Map.Entry<String, List<String>> entry : allData.entrySet()) {
            String key = entry.getKey();
            List<String> allFlights = entry.getValue();
            Collections.shuffle(allFlights);
            Set<String> flightset = new HashSet<>();
            int total = (int) (entry.getValue().size() * PERCENTAGE_OF_DATASET_TO_USE);
            while(flightset.size() < total) {
                flightset.add(allFlights.get(random.nextInt(allFlights.size())));
            }
            sample.put(key, new ArrayList<>(flightset));
        }
        return sample;
    }

}
