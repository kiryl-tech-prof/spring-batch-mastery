package org.example;


// Entity representing team's performance, expressed as a string
public class TeamPerformance {

    private final String name;
    private final String performance;

    public TeamPerformance(String name, String performance) {
        this.name = name;
        this.performance = performance;
    }

    public String getName() {
        return name;
    }

    public String getPerformance() {
        return performance;
    }
}
