package org.example;

import org.springframework.batch.item.ItemProcessor;


// Processor which takes raw data and aggregates that
public class RawToAggregateSensorDataProcessor implements ItemProcessor<DailySensorData, DailyAggregatedSensorData> {

    @Override
    public DailyAggregatedSensorData process(DailySensorData item) throws Exception {
        double min = item.getMeasurements().get(0);
        double max = min;
        double sum = 0;

        for (double measurement : item.getMeasurements()) {
            min = Math.min(min, measurement);
            max = Math.max(max, measurement);
            sum += measurement;
        }

        double avg = sum / item.getMeasurements().size();

        return new DailyAggregatedSensorData(item.getDate(), convertToCelsius(min), convertToCelsius(avg), convertToCelsius(max));
    }

    private static double convertToCelsius(double fahT) {
        return (5 * (fahT - 32)) / 9;
    }
}
