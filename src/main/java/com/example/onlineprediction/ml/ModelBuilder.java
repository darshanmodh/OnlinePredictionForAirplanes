package com.example.onlineprediction.ml;

import com.example.onlineprediction.data.DataLoader;
import org.apache.mahout.classifier.evaluation.Auc;
import org.apache.mahout.classifier.sgd.L1;
import org.apache.mahout.classifier.sgd.OnlineLogisticRegression;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.security.SecureRandom;
import java.util.*;

public class ModelBuilder {

    private static final int NUM_EPOCS = 20;
    private static final double PERCENTAGE_OF_DATASET_TO_USE = 0.50;
    private static final double TRAINING_PERCENTAGE = 0.40;
    private static final Logger LOG = LoggerFactory.getLogger(ModelBuilder.class);
    private static final String PATH = "src/main/resources/flights.csv";
    private static final String INCOMING_FILEPATH = "src/main/resources/august_2017.csv";

    public static void main(String[] args) throws IOException {
        LOG.info("Training Now. Called from command line.");
        train(PATH);
    }

    public static Map<String, OnlineLogisticRegression> train(String path) throws IOException {
        final Map<String, OnlineLogisticRegression> regressionMap = new HashMap<>();

        final Map<String, List<String>> data = DataLoader.getFlightDataByAirport(path);

        final Map<String, List<String>> sample = getRandomSampling(data);

        final List<FlightData> flightData = new ArrayList<>();

        for(Map.Entry<String, List<String>> entry : sample.entrySet()) {
            List<String> airportValues = entry.getValue();
            int trainIndex = (int) (airportValues.size() * TRAINING_PERCENTAGE);
            List<String> train = airportValues.subList(0, trainIndex);
            List<String> test = airportValues.subList(trainIndex + 1, airportValues.size());
            for(String flight : train) {
                flightData.add(new FlightData(flight));
            }
            LOG.info("training for {}", entry.getKey());
            OnlineLogisticRegression trainedRegression = onlineRegression(flightData);
            LOG.info("training completed.");
            testTrainedRegression(trainedRegression, entry.getKey(), test);
            regressionMap.put(entry.getKey(), trainedRegression);
        }

        return regressionMap;
    }

    private static void testTrainedRegression(OnlineLogisticRegression trainedRegression, String key, List<String> testFlights) {
        Auc eval = new Auc(0.5);
        for(String flight : testFlights) {
            FlightData flightData = new FlightData(flight);
            eval.add(flightData.realResult, trainedRegression.classifyScalar(flightData.vector));
        }
        LOG.info("Training accuracy for {} {}", key, eval.auc());
    }

    private static OnlineLogisticRegression onlineRegression(List<FlightData> allFlightData) {
        OnlineLogisticRegression logisticRegression = new OnlineLogisticRegression(2, FlightData.NUM_FEATURES, new L1())
                .learningRate(1)
                .alpha(1)
                .lambda(0.000001)
                .stepOffset(10000)
                .decayExponent(0.2);

        for(int i=0; i<NUM_EPOCS; i++) {
            for(FlightData flightData : allFlightData) {
                logisticRegression.train(flightData.realResult, flightData.vector);
            }
        }
        return logisticRegression;
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

    // called from DataProducer
    public static Map<String,byte[]> buildModel(String path) throws IOException {
        Map<String, byte[]> coefficientMap = new HashMap<>();
        Map<String, OnlineLogisticRegression> airlineData = train(path);
        for(Map.Entry<String, OnlineLogisticRegression> regressionEntry : airlineData.entrySet()) {
            coefficientMap.put(regressionEntry.getKey(), getBytesFromOnlineRegression(regressionEntry.getValue()));
        }
        return coefficientMap;
    }

    private static byte[] getBytesFromOnlineRegression(OnlineLogisticRegression logisticRegression) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        logisticRegression.write(dos);
        return baos.toByteArray();
    }
}
