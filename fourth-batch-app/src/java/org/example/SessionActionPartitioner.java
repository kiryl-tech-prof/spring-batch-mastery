package org.example;

import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.item.ExecutionContext;

import java.util.HashMap;
import java.util.Map;


/**
 * Partitioner that sets up partition count and index to all the partitions to process session actions
 */
public class SessionActionPartitioner implements Partitioner {

    public static final String PARTITION_COUNT = "partitionCount";
    public static final String PARTITION_INDEX = "partitionIndex";
    private static final String PARTITION_NAME_PREFIX = "sessionActionPartition-";

    @Override
    public Map<String, ExecutionContext> partition(int gridSize) {
        Map<String, ExecutionContext> partitionMap = new HashMap<>(gridSize);
        for (int i = 0; i < gridSize; i++) {
            ExecutionContext executionContext = new ExecutionContext();
            executionContext.putInt(PARTITION_COUNT, gridSize);
            executionContext.putInt(PARTITION_INDEX, i);
            partitionMap.put(PARTITION_NAME_PREFIX + i, executionContext);
        }
        return partitionMap;
    }
}
