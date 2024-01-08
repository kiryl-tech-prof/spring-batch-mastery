package org.example;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.partition.support.AbstractPartitionHandler;
import org.springframework.batch.core.repository.JobRepository;

import java.net.URI;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;


/**
 * Partition handler that
 *  - starts partition workers through http requests with no retry
 *    and very simple error handling logic
 *  - receives responses from workers a form of "yes, it's started",
 *    so immediately not knowing the status of the execution of each worker;
 *    implemented at {@link ApplicationController#startWorker(long, long, String)}
 *  - after all workers are started, queries job repository db periodically
 *    to get acknowledgement about the worker status; return when all statuses are
 *    clear (either success or not) or on timeout (though, not doing any effort to
 *    cancel already running workers)
 */
public class HttpRequestPartitionHandler extends AbstractPartitionHandler {

    private final Step workerStep;
    private final PartitioningConfig partitioningConfig;
    private final long endToEndTimeoutMillis;
    private final JobRepository jobRepository;
    private final JobExplorer jobExplorer;

    private static final CloseableHttpClient HTTP_CLIENT = HttpClientBuilder.create().build();
    private static final String START_WORKER_ENDPOINT = "start-worker";
    private static final long NANO_IN_MILLI = 1000000;

    public HttpRequestPartitionHandler(Step workerStep, PartitioningConfig partitioningConfig, long endToEndTimeoutMillis,
                                       JobRepository jobRepository, JobExplorer jobExplorer) {
        this.workerStep = workerStep;
        this.partitioningConfig = partitioningConfig;
        this.endToEndTimeoutMillis = endToEndTimeoutMillis;
        this.jobRepository = jobRepository;
        this.jobExplorer = jobExplorer;
        this.gridSize = partitioningConfig.getWorkerBaseUrls().length;
    }

    @Override
    protected Set<StepExecution> doHandle(StepExecution managerStepExecution, Set<StepExecution> partitionStepExecutions) throws Exception {
        if (partitioningConfig.getWorkerBaseUrls().length != partitionStepExecutions.size()) {
            // Ideally should not happen, since we trust to StepExecutionSplitter to use grid size properly
            throw new IllegalArgumentException("Misconfiguration: grid size" + partitionStepExecutions.size() +  " should be equal to the number of workers " + partitioningConfig.getWorkerBaseUrls().length + "  provided through partitioning config");
        }

        Iterator<StepExecution> stepExecutionIterator = partitionStepExecutions.iterator();
        // Start all workers
        for (String workerBaseUrl : partitioningConfig.getWorkerBaseUrls()) {
            sendStartWorkerRequest(workerBaseUrl, stepExecutionIterator.next());
        }

        // Poll job repository (Spring Batch DB) periodically every second until either completion happen,
        // or the timeout interval has passed
        long startTime = System.nanoTime();
        while ((System.nanoTime() - startTime) / NANO_IN_MILLI < endToEndTimeoutMillis) {
            // In case all partition step executions are done, time to return
            if (checkPartitionStepExecutionCompleted(managerStepExecution, partitionStepExecutions)) {
                return partitionStepExecutions;
            } else {
                // Wait for 1 second for the job to be completed
                Thread.sleep(1000);
            }
        }
        throw new RuntimeException("HTTP request partition handler timed out");
    }

    private void sendStartWorkerRequest(String workerBaseUrl, StepExecution partitionStepExecution) throws Exception {
        URI uri = new URIBuilder(workerBaseUrl + START_WORKER_ENDPOINT)
                .addParameter("jobExecutionId", Long.toString(partitionStepExecution.getJobExecutionId()))
                .addParameter("stepExecutionId", Long.toString(partitionStepExecution.getId()))
                .addParameter("stepName", workerStep.getName())
                .build();
        CloseableHttpResponse response = HTTP_CLIENT.execute(new HttpPost(uri));
        // In case of failure, we set step execution's status to FAILED
        if (response.getStatusLine().getStatusCode() != 200) {
            ExitStatus exitStatus = ExitStatus.FAILED
                    .addExitDescription("HTTP request to start worker did not finish successfully, so exiting");
            partitionStepExecution.setStatus(BatchStatus.FAILED);
            partitionStepExecution.setExitStatus(exitStatus);
            jobRepository.update(partitionStepExecution); // Persist!
        }
    }

    private boolean checkPartitionStepExecutionCompleted(StepExecution managerStepExecution, Set<StepExecution> partitionStepExecutions) {
        // Load job execution and all corresponding step executions
        JobExecution jobExecution = jobExplorer.getJobExecution(managerStepExecution.getJobExecutionId());
        Map<Long, BatchStatus> stepExecutionStatusMap = new HashMap<>(jobExecution.getStepExecutions().size());
        // Create a map with step execution id to its status (fresh one, from the db)
        for (StepExecution queriedStepExecution : jobExecution.getStepExecutions()) {
            stepExecutionStatusMap.put(queriedStepExecution.getId(), queriedStepExecution.getStatus());
        }

        boolean jobComplete = true;
        // Check step execution statuses by id, to see whether all of them are completed
        for (StepExecution partitionStepExecution : partitionStepExecutions) {
            BatchStatus partitionStepStatus = stepExecutionStatusMap.get(partitionStepExecution.getId());
            partitionStepExecution.setStatus(partitionStepStatus); // Also, setting proper status such that it could be used upstream
            // We only check for COMPLETED and FAILED, but in reality a bit more sophisticated logic is needed
            // To cover all the use cases, we need to take care of statuses like STOPPED (ideally)
            if (!BatchStatus.COMPLETED.equals(partitionStepStatus) && !BatchStatus.FAILED.equals(partitionStepStatus)) {
                jobComplete = false;
            }
        }
        return jobComplete;
    }
}
