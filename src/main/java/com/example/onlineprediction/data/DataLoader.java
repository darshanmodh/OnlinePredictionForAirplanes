package com.example.onlineprediction.data;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import static java.util.stream.Collectors.groupingBy;

public class DataLoader {

    private static final Logger LOG = LoggerFactory.getLogger(DataLoader.class);

    private static final String PATH = "src/main/resources/flights.csv";

    private static final int AIRPORT_INDEX = 4;

    private static final Pattern TOP_15_BUSIEST_AIRPORTS = Pattern.compile("\"(SLC|SEA|EWR|MCO|BOS|CLT|LAS|PHX|SFO|IAH|LAX|DEN|DFW|ORD|ATL)\"");

    private static Function<Integer, Predicate<String>> airportMatcher =
            index -> line -> TOP_15_BUSIEST_AIRPORTS.matcher(line.split(",")[index]).matches();

    private static BiFunction<Integer, String, String> getFieldAt = (index, line) -> line.split(",")[index];

    private static Function<String, String> cleanQuotes = line -> line.replaceAll("\"", "");

    public static void main(String[] args) throws IOException {
        LOG.info("Getting Flights");
        Map<String, List<String>> trainingData = getFlightDataByAirport(PATH);
        printMap(trainingData);
    }

    private static void printMap(Map<String, List<String>> trainingData) {
        for(Map.Entry<String, List<String>> data : trainingData.entrySet()) {
            LOG.info("{} number flights {}", data.getKey(), data.getValue().size());
        }
    }

    public static Map<String,List<String>> getFlightDataByAirport(String path) throws IOException {

        return Files.readAllLines(new File(path).toPath())
                .stream()
                .filter(airportMatcher.apply(AIRPORT_INDEX))
                .map(cleanQuotes)
                .collect(groupingBy(line -> getFieldAt.apply(AIRPORT_INDEX, line)));
    }

}
