package org.example;


// Container class for the array of worker server URL, bootstrapped from 'partitioning.properties'
public class PartitioningConfig {

    private final String[] workerBaseUrls;

    public PartitioningConfig(String workerBaseUrlsProperty) {
        workerBaseUrls = workerBaseUrlsProperty.split(",");
    }

    public String[] getWorkerBaseUrls() {
        return workerBaseUrls;
    }
}
