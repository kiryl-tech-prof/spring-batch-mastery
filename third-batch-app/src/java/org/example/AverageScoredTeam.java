package org.example;


// Entity for team name and corresponding average score
public class AverageScoredTeam {

    private final String name;
    private final double averageScore;

    public AverageScoredTeam(String name, double averageScore) {
        this.name = name;
        this.averageScore = averageScore;
    }

    public String getName() {
        return name;
    }

    public double getAverageScore() {
        return averageScore;
    }
}
