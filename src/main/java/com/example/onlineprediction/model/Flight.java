package com.example.onlineprediction.model;

import static com.example.onlineprediction.data.Fields.*;

public class Flight {

    private final String month;
    private final String day;
    private final String flightNumber;
    private final String time;
    private final String airLine;
    private final String actualDelay;

    public Flight(String data) {
        String[] parts = data.split(",");
        this.month = parts[MONTH.ordinal()];
        this.day = parts[DAY_OF_WEEK.ordinal()];
        this.flightNumber = parts[FL_NUM.ordinal()];
        this.time = parts[CRS_DEP_TIME.ordinal()];
        this.airLine = parts[UNIQUE_CARRIER.ordinal()];
        this.actualDelay = parts[ARR_DELAY.ordinal()];
    }

    @Override
    public String toString() {
        return "Flight{" +
                "month='" + month + '\'' +
                ", day='" + day + '\'' +
                ", flightNumber='" + flightNumber + '\'' +
                ", time='" + time + '\'' +
                ", airLine='" + airLine + '\'' +
                ", actualDelay='" + actualDelay + '\'' +
                '}';
    }
}
