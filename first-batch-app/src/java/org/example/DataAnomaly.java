package org.example;


// Data model class for detected anomaly in aggregated sensor data
public class DataAnomaly {

    // Represented as 'MM-dd-yyyy'
    private String date;
    private AnomalyType type;
    private double value;

    public DataAnomaly(String date, AnomalyType type, double value) {
        this.date = date;
        this.type = type;
        this.value = value;
    }

    public String getDate() {
        return date;
    }

    public AnomalyType getType() {
        return type;
    }

    public double getValue() {
        return value;
    }
}
