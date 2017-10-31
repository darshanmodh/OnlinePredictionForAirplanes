package com.example.onlineprediction.ml;

import com.example.onlineprediction.model.DataRegression;
import com.example.onlineprediction.model.Flight;
import org.apache.mahout.classifier.sgd.OnlineLogisticRegression;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;

public class Predictor {
    public static final Logger LOG = LoggerFactory.getLogger(Predictor.class);

    public static String predict(DataRegression dataRegression) {
        try (OnlineLogisticRegression logisticRegression = new OnlineLogisticRegression()) {
            FlightData flightData = new FlightData(dataRegression.data);
            logisticRegression.readFields(new DataInputStream(new ByteArrayInputStream(dataRegression.coefficients)));
            double prediction = logisticRegression.classifyScalar(flightData.vector);
            String arrivalPrediction = prediction > 0.5 ? "on-time" : "late";
            return String.format("%s predicted to %s", new Flight(dataRegression.data), arrivalPrediction);
        } catch (Exception e) {
            LOG.error("Problem with predicting {} {}", dataRegression.data, e);
            return null;
        }
    }
}
