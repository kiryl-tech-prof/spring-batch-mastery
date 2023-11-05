package org.example;

import org.springframework.batch.item.ItemProcessor;


// Processor which checks aggregated data for anomalies, filters out all normal data and creates anomaly records
public class SensorDataAnomalyProcessor implements ItemProcessor<DailyAggregatedSensorData, DataAnomaly> {

    // Threshold for low / high ratio to be considered anomaly vs normal
    private static final double THRESHOLD = 0.9;

    @Override
    public DataAnomaly process(DailyAggregatedSensorData item) throws Exception {
        if ((item.getMin() / item.getAvg()) < THRESHOLD) {
            return new DataAnomaly(item.getDate(), AnomalyType.MINIMUM, item.getMin());
        } else if ((item.getAvg() / item.getMax()) < THRESHOLD) {
            return new DataAnomaly(item.getDate(), AnomalyType.MAXIMUM, item.getMax());
        } else {
            // Convention is to return null to filter item out and not pass it to the writer
            return null;
        }
    }
}
