package org.example;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.job.AbstractJob;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.sql.DataSource;
import java.util.UUID;


@RestController
public class ApplicationController {

    private static final Logger LOGGER = LoggerFactory.getLogger(ApplicationController.class);

    @Autowired
    private JobExplorer jobExplorer;

    @Autowired
    @Qualifier("asyncJobLauncher")
    private JobLauncher jobLauncher;

    @Autowired
    @Qualifier("simpleActionCalculationJob")
    private AbstractJob simpleActionCalculationJob;

    @Autowired
    @Qualifier("multiThreadedActionCalculationJob")
    private Job multiThreadedActionCalculationJob;

    @Autowired
    @Qualifier("partitionedLocalActionCalculationJob")
    public Job partitionedLocalActionCalculationJob;

    @Autowired
    @Qualifier("partitionedRemoteActionCalculationJob")
    public Job partitionedRemoteActionCalculationJob;

    @Autowired
    private JobRepository jobRepository;

    @Autowired
    @Qualifier("sourceDataSource")
    public DataSource sourceDataSource;

    // Task executor to launch worker partition step executions asynchronously
    private final TaskExecutor taskExecutor = new SimpleAsyncTaskExecutor();


    @PostMapping("/start-simple-local")
    public String startSimpleLocal() throws Exception {
        prepareEmptyResultTable();
        jobLauncher.run(simpleActionCalculationJob, buildUniqueJobParameters());
        return "Successfully started!\n";
    }

    @PostMapping("/start-multi-threaded")
    public String startMultiThreaded() throws Exception {
        prepareEmptyResultTable();
        jobLauncher.run(multiThreadedActionCalculationJob, buildUniqueJobParameters());
        return "Successfully started!\n";
    }

    @PostMapping("/start-partitioned-local")
    public String startPartitionedLocal() throws Exception {
        prepareEmptyResultTable();
        jobLauncher.run(partitionedLocalActionCalculationJob, buildUniqueJobParameters());
        return "Successfully started!\n";
    }

    @PostMapping("/start-partitioned-remote")
    public String startPartitionedRemote() throws Exception {
        prepareEmptyResultTable();
        jobLauncher.run(partitionedRemoteActionCalculationJob, buildUniqueJobParameters());
        return "Successfully started!\n";
    }

    // This endpoint should be called by the manager of partitioned distributed execution
    @PostMapping("/start-worker")
    public void startWorker(@RequestParam("jobExecutionId") long jobExecutionId,
                            @RequestParam("stepExecutionId") long stepExecutionId,
                            @RequestParam("stepName") String stepName) throws Exception {
        LOGGER.info("Worker endpoint is requested and about to start to execute the partition");
        LOGGER.info("Job execution id: " + jobExecutionId);
        LOGGER.info("Step execution id: " + stepExecutionId);
        LOGGER.info("Step name: " + stepName);

        startWorkerPartitionExecutionAsync(jobExecutionId, stepExecutionId, stepName);
    }

    // Starts execution of the specific step for the specific partition asynchronously
    @SuppressWarnings("all") // Suppressing warnings since AbstractJob.getStep() is defined and implemented ambiguously
    private void startWorkerPartitionExecutionAsync(long jobExecutionId, long stepExecutionId, String stepName) {
        StepExecution stepExecution = jobExplorer.getStepExecution(jobExecutionId, stepExecutionId);
        if (stepExecution == null) {
            throw new IllegalArgumentException("No step execution exist for job execution id = " + jobExecutionId +
                    " and step execution id = " + stepExecutionId);
        }

        Step step = simpleActionCalculationJob.getStep(stepName);
        if (step == null) {
            throw new IllegalArgumentException("No step with name '" + stepName + "' exist");
        }

        // Submit step to be executed
        taskExecutor.execute(() -> {
            // Handling failure scenarios, since when manager of all this distributed execution
            // aggregates the result, they will need to know the status
            try {
                step.execute(stepExecution);

                LOGGER.info("Worker successfully completed the execution of the partition");
                LOGGER.info("Job execution id: " + jobExecutionId);
                LOGGER.info("Step execution id: " + stepExecutionId);
                LOGGER.info("Step name: " + stepName);
            } catch (Throwable e) {
                stepExecution.addFailureException(e);
                stepExecution.setStatus(BatchStatus.FAILED);
                jobRepository.update(stepExecution);
            }
        });
    }

    // Method which destroys and then creates an empty user score table to store the results into
    private void prepareEmptyResultTable() {
        JdbcTemplate jdbcTemplate = new JdbcTemplate(sourceDataSource);
        SourceDatabaseUtils.dropTableIfExists(jdbcTemplate, UserScoreUpdate.USER_SCORE_TABLE_NAME);
        SourceDatabaseUtils.createUserScoreTable(jdbcTemplate, UserScoreUpdate.USER_SCORE_TABLE_NAME);
    }

    // Building unique job parameters to not care about restarts
    private static JobParameters buildUniqueJobParameters() {
        return new JobParametersBuilder()
                .addString(UUID.randomUUID().toString(), UUID.randomUUID().toString())
                .toJobParameters();
    }
}
