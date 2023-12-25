package org.example;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;


@RestController
@EnableScheduling
public class ApplicationController {

    @Autowired
    @Qualifier("asyncJobLauncher")
    private JobLauncher jobLauncher;

    @Autowired
    @Qualifier("teamPerformanceJob")
    private Job teamPerformanceJob;

    @PostMapping("/start") // Endpoint to asynchronously start the job
    public String start(@RequestParam("scoreRank") int scoreRank) throws Exception {
        String uuid = UUID.randomUUID().toString();
        launchJobAsynchronously(scoreRank, uuid);
        return "Job with id " + uuid + " was submitted";
    }

//    @Scheduled(cron = "0 0/30 * * * ?") // Please uncomment for scheduled behavior
    public void scheduledJobStarter() throws Exception {
        // For scheduled job, we are interested in best scores, hence rank = 0
        launchJobAsynchronously(0, UUID.randomUUID().toString());
    }

    // Launches jobs using async job launcher and exits
    private void launchJobAsynchronously(int scoreRank, String uuid) throws Exception {
        jobLauncher.run(teamPerformanceJob, new JobParametersBuilder()
                .addLong(TeamPerformanceJobConfiguration.SCORE_RANK_PARAM, (long) scoreRank)
                .addString(TeamPerformanceJobConfiguration.UUID_PARAM, uuid)
                .toJobParameters());
    }
}
