package org.example;

import java.util.List;


// Domain model representing single row of raw input text file
public class DailySensorData {

    // Represented as 'MM-dd-yyyy'
    private final String date;
    // List representing measurements in Fahrenheit
    private final List<Double> measurements;

    public DailySensorData(String date, List<Double> measurements) {
        this.date = date;
        this.measurements = measurements;
    }

    public String getDate() {
        return date;
    }

    public List<Double> getMeasurements() {
        return measurements;
    }
}
