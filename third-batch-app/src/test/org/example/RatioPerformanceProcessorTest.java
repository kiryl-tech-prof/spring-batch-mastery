package org.example;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.test.StepScopeTestUtils;
import org.springframework.batch.test.context.SpringBatchTest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import java.util.UUID;

import static org.mockito.Mockito.*;


// Unit test for 'maxRatioPerformanceProcessor' and 'minRatioPerformanceProcessor' beans
@SpringBatchTest
@SpringJUnitConfig({TeamPerformanceJobConfiguration.class, TestConfiguration.class})
public class RatioPerformanceProcessorTest {

    private static final String TEAM_NAME = UUID.randomUUID().toString();
    private static final double TEAM_SCORE = 4d;
    private static final String EXPECTED_MIN_PERFORMANCE = "200.00%";
    private static final double MIN_SCORE = 2d;
    private static final String EXPECTED_MAX_PERFORMANCE = "80.00%";
    private static final double MAX_SCORE = 5d;

    @Autowired
    @Qualifier("maxRatioPerformanceProcessor")
    private ItemProcessor<AverageScoredTeam, TeamPerformance> maxRatioPerformanceProcessor;

    @Autowired
    @Qualifier("minRatioPerformanceProcessor")
    private ItemProcessor<AverageScoredTeam, TeamPerformance> minRatioPerformanceProcessor;

    @Test
    public void testProcessorsUsingSharedStepExecution() throws Exception {
        AverageScoredTeam team = new AverageScoredTeam(TEAM_NAME, TEAM_SCORE);
        TeamPerformance minPerformance = minRatioPerformanceProcessor.process(team);
        Assertions.assertNotNull(minPerformance);
        Assertions.assertEquals(minPerformance.getName(), TEAM_NAME);
        Assertions.assertEquals(minPerformance.getPerformance(), EXPECTED_MIN_PERFORMANCE);

        TeamPerformance maxPerformance = maxRatioPerformanceProcessor.process(team);
        Assertions.assertNotNull(maxPerformance);
        Assertions.assertEquals(maxPerformance.getName(), TEAM_NAME);
        Assertions.assertEquals(maxPerformance.getPerformance(), EXPECTED_MAX_PERFORMANCE);
    }

    // Custom step execution is provided for common use across all tests
    public StepExecution getStepExecution() {
        // Mocking job execution context parameters
        StepExecution stepExecution = mock(StepExecution.class);
        JobExecution jobExecution = mock(JobExecution.class);
        when(stepExecution.getJobExecution()).thenReturn(jobExecution);

        ExecutionContext context = new ExecutionContext();
        context.putDouble(TeamAverageProcessor.MIN_SCORE, MIN_SCORE);
        context.putDouble(TeamAverageProcessor.MAX_SCORE, MAX_SCORE);
        when(jobExecution.getExecutionContext()).thenReturn(context);

        return stepExecution;
    }

    @Test
    public void testProcessorsUsingCustomStepExecution() throws Exception {
        // Creating custom step execution mock to use it
        StepExecution stepExecution = mock(StepExecution.class);
        JobExecution jobExecution = mock(JobExecution.class);
        when(stepExecution.getJobExecution()).thenReturn(jobExecution);

        ExecutionContext context = new ExecutionContext();
        context.putDouble(TeamAverageProcessor.MIN_SCORE, 1d);
        context.putDouble(TeamAverageProcessor.MAX_SCORE, 2d);
        when(jobExecution.getExecutionContext()).thenReturn(context);

        AverageScoredTeam team = new AverageScoredTeam(TEAM_NAME, 4d);

        // We make sure to bring-our-own step execution
        TeamPerformance minPerformance = StepScopeTestUtils
                .doInStepScope(stepExecution, () -> minRatioPerformanceProcessor.process(team));
        Assertions.assertNotNull(minPerformance);
        Assertions.assertEquals(minPerformance.getName(), TEAM_NAME);
        Assertions.assertEquals(minPerformance.getPerformance(), "400.00%");

        // We make sure to bring-our-own step execution
        TeamPerformance maxPerformance = StepScopeTestUtils
                .doInStepScope(stepExecution, () -> maxRatioPerformanceProcessor.process(team));
        Assertions.assertNotNull(maxPerformance);
        Assertions.assertEquals(maxPerformance.getName(), TEAM_NAME);
        Assertions.assertEquals(maxPerformance.getPerformance(), "200.00%");
    }
}
