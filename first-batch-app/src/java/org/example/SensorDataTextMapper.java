package org.example;

import org.springframework.batch.item.file.LineMapper;

import java.util.Arrays;
import java.util.stream.Collectors;


/**
 * Implementation of {@link LineMapper} for raw sensor data format
 */
public class SensorDataTextMapper implements LineMapper<DailySensorData> {

    @Override
    public DailySensorData mapLine(String line, int lineNumber) throws Exception {
        String[] dateAndMeasurements = line.split(":"); // Split into date and measurements
        return new DailySensorData(dateAndMeasurements[0],
                                   // Split measurements by comma
                                   Arrays.stream(dateAndMeasurements[1].split(","))
                                           // Convert each string to double
                                          .map(Double::parseDouble)
                                           // Collect stream to list
                                          .collect(Collectors.toList()));

    }
}
