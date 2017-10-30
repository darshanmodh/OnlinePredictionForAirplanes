package com.example.onlineprediction.ml;

import com.example.onlineprediction.data.Fields;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.vectorizer.encoders.ConstantValueEncoder;
import org.apache.mahout.vectorizer.encoders.ContinuousValueEncoder;
import org.apache.mahout.vectorizer.encoders.FeatureVectorEncoder;
import org.apache.mahout.vectorizer.encoders.StaticWordValueEncoder;

import static com.example.onlineprediction.data.Fields.*;

public class FlightData {

    public static final int NUM_FEATURES = 6;
    public static final RandomAccessSparseVector vector = new RandomAccessSparseVector(NUM_FEATURES);
    public final int realResult;
    private final ConstantValueEncoder bias = new ConstantValueEncoder("bias");
    private final FeatureVectorEncoder categoryValueEncoder = new StaticWordValueEncoder("categories");
    private final ContinuousValueEncoder numericEncoder = new ContinuousValueEncoder("numbers");

    public FlightData(String data) {
        String[] parts = data.split(",");

        String late = parts[ARR_DELAY.ordinal()];
        late = late.isEmpty() ? "0.0" : late;
        realResult = Double.parseDouble(late) == 0.0 ? 1 : 0;
        bias.addToVector("1", vector);

        for(Fields field : Fields.values()) {
            switch(field) {
                case DAY_OF_WEEK:
                case UNIQUE_CARRIER:
                case ORIGIN:
                case DEST:
                    categoryValueEncoder.addToVector(parts[field.ordinal()], vector);
                    break;
                case DISTANCE:
                    Double distance = Double.parseDouble(parts[DISTANCE.ordinal()]) / 100000;
                    numericEncoder.addToVector(distance.toString(), vector);
                    break;
            }
        }
    }
}
