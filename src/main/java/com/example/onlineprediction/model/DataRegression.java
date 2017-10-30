package com.example.onlineprediction.model;

import java.util.Arrays;

public class DataRegression {

    public final String data;
    public final byte[] coefficients;

    public DataRegression(String data, byte[] coefficients) {
        this.data = data;
        this.coefficients = coefficients;
    }

    @Override
    public String toString() {
        return "DataRegression{" +
                "data='" + data + '\'' +
                ", coefficients=" + Arrays.toString(coefficients) +
                '}';
    }

}
