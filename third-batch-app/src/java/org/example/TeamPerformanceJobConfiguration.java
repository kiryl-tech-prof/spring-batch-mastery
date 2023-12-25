package org.example;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.job.flow.support.SimpleFlow;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.TaskExecutorJobLauncher;
import org.springframework.batch.core.listener.ExecutionContextPromotionListener;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.step.tasklet.CommandRunner;
import org.springframework.batch.core.step.tasklet.JvmCommandRunner;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.file.FlatFileHeaderCallback;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.batch.item.file.builder.MultiResourceItemReaderBuilder;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.batch.BatchDataSourceScriptDatabaseInitializer;
import org.springframework.boot.autoconfigure.batch.BatchProperties;
import org.springframework.boot.sql.init.DatabaseInitializationMode;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.WritableResource;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.io.Writer;
import java.math.BigDecimal;
import java.math.RoundingMode;


@Configuration
@EnableBatchProcessing
@PropertySource("classpath:db.properties")
public class TeamPerformanceJobConfiguration {

    private static final Logger LOGGER = LoggerFactory.getLogger(TeamPerformanceJobConfiguration.class);
    // Job parameter names
    public static final String SCORE_RANK_PARAM = "scoreRank";
    public static final String UUID_PARAM = "uuid";

    @Value("classpath:input/*.txt")
    private Resource[] inDivisionResources;

    @Value("file:calculated/avg.txt")
    private WritableResource outAvgResource;

    @Value("file:calculated/max.txt")
    private WritableResource maxPerformanceRatioOutResource;

    @Value("file:calculated/min.txt")
    private WritableResource minPerformanceRatioOutResource;

    @Value("file:calculated/")
    private WritableResource calculatedDirectoryResource;

    @Bean
    @Qualifier("teamPerformanceJob")
    public Job teamPerformanceJob(JobRepository jobRepository,
                                  @Qualifier("threadPoolTaskExecutor") TaskExecutor threadPoolTaskExecutor,
                                  @Qualifier("averageTeamScoreStep") Step averageTeamScoreStep,
                                  @Qualifier("teamMaxRatioPerformanceStep") Step teamMaxRatioPerformanceStep,
                                  @Qualifier("teamMinRatioPerformanceStep") Step teamMinRatioPerformanceStep,
                                  @Qualifier("shellScriptStep") Step shellScriptStep,
                                  @Qualifier("successLoggerStep") Step successLoggerStep) {
        // Wrap both performance steps into corresponding flows
        Flow maxRatioPerformanceFlow = new FlowBuilder<SimpleFlow>("maxRatioPerformanceFlow")
                .start(teamMaxRatioPerformanceStep).build();
        Flow minRatioPerformanceFlow = new FlowBuilder<SimpleFlow>("minRatioPerformanceFlow")
                .start(teamMinRatioPerformanceStep).build();

        // Split flow to execute min and max ratio performance flows in parallel
        Flow performanceSplitFlow = new FlowBuilder<SimpleFlow>("performanceSplitFlow")
                .split(threadPoolTaskExecutor)
                .add(maxRatioPerformanceFlow, minRatioPerformanceFlow)
                .build();

        // Now, hook everything together
        return new JobBuilder("teamPerformanceJob", jobRepository)
                // 1. (Start) Flow with single step -> average team score
                // (flow is needed since the next is split flow, not a step)
                .start(new FlowBuilder<SimpleFlow>("averageTeamScoreFlow")
                        .start(averageTeamScoreStep)
                        .build())
                // 2. Next is parallel flow with 2 performance steps running in parallel
                .next(performanceSplitFlow)
                // 3. Execute shell script after done with parallel performance steps
                .next(shellScriptStep)
                // 4. Last step is to execute logging the success step
                .next(successLoggerStep)
                .build()
                .build();
    }

    @Bean
    @Qualifier("threadPoolTaskExecutor")
    public TaskExecutor threadPoolTaskExecutor() {
        ThreadPoolTaskExecutor threadPool = new ThreadPoolTaskExecutor();
        // 2-thread size pool to execute split flow with 2 parallel steps
        threadPool.setCorePoolSize(2);
        return threadPool;
    }

    @Bean
    @Qualifier("averageTeamScoreStep")
    public Step averageTeamScoreStep(JobRepository jobRepository, PlatformTransactionManager transactionManager,
                                     @Qualifier("divisionTeamReader") ItemReader<Team> divisionTeamReader,
                                     @Qualifier("teamAverageProcessor") TeamAverageProcessor teamAverageProcessor,
                                     @Qualifier("teamAverageContextPromotionListener") ExecutionContextPromotionListener teamAverageContextPromotionListener,
                                     @Qualifier("jobStartLoggerListener") StepExecutionListener jobStartLoggerListener) {
        return new StepBuilder("averageTeamScoreStep", jobRepository)
                // Read-and-write one-by-one
                .<Team, AverageScoredTeam>chunk(1, transactionManager)
                // Supplying autowired multi-file multi-line reader
                .reader(divisionTeamReader)
                // Processor to calculate averages for specific rank
                .processor(teamAverageProcessor)
                // Writing team and average score in comma-separated format
                .writer(new FlatFileItemWriterBuilder<AverageScoredTeam>()
                        .name("averageTeamScoreWriter")
                        .resource(outAvgResource)
                        .delimited()
                        .delimiter(",")
                        .fieldExtractor(team -> new Object[] {team.getName(), team.getAverageScore()})
                        .build())
                // This step should log the informational message
                .listener(jobStartLoggerListener)
                // Listener to promote step execution context to job execution context
                .listener(teamAverageContextPromotionListener)
                // Listener to inject step execution object before step started and to flush it after it finished
                .listener(new StepExecutionListener() {
                    @Override
                    public void beforeStep(StepExecution stepExecution) {
                        teamAverageProcessor.setStepExecution(stepExecution);
                    }

                    @Override
                    public ExitStatus afterStep(StepExecution stepExecution) {
                        teamAverageProcessor.setStepExecution(null);
                        return StepExecutionListener.super.afterStep(stepExecution);
                    }
                })
//                .faultTolerant()  //Skipping behavior could be enabled through fault-tolerant chaining
//                .skip(IndexOutOfBoundsException.class)
//                .skipLimit(40)
//                .listener(new SkipListener<Team, AverageScoredTeam>() {
//                    @Override
//                    public void onSkipInProcess(Team team, Throwable t) {
//                        LOGGER.error("Error while processing team " + team.getName() + " , item is skipped");
//                        LOGGER.error("Reason: " + t.getClass().getName() + " -> " + t.getLocalizedMessage());
//                    }
//                })
                .build();
    }

    @Bean
    @Qualifier("divisionTeamReader")
    public ItemReader<Team> divisionTeamReader() {
        FlatFileItemReader<String> lineReader = new FlatFileItemReaderBuilder<String>()
                .name("divisionLineReader")
                // Mapping line into line (no-op)
                .lineMapper((line, lineNumber) -> line)
                .build();

        // Reader that relies on line-by-line reader, but able to comprehend multi-line team records
        DivisionFileReader singleFileMultiLineReader = new DivisionFileReader(lineReader);

        // Reader that relies on single file reader, but able to read from the directory
        return new MultiResourceItemReaderBuilder<Team>()
                .name("divisionTeamReader")
                .delegate(singleFileMultiLineReader)
                .resources(inDivisionResources)
                .build();
    }

    @Bean
    @StepScope
    @Qualifier("teamAverageProcessor")
    public TeamAverageProcessor teamAverageProcessor(@Value("#{jobParameters['scoreRank']}") int scoreRank) {
        return new TeamAverageProcessor(scoreRank);
    }

    @Bean
    @Qualifier("teamAverageContextPromotionListener")
    public ExecutionContextPromotionListener teamAverageContextPromotionListener() {
        ExecutionContextPromotionListener listener = new ExecutionContextPromotionListener();
        listener.setKeys(new String[]
                {TeamAverageProcessor.MAX_SCORE, TeamAverageProcessor.MAX_PLAYER,
                 TeamAverageProcessor.MIN_SCORE, TeamAverageProcessor.MIN_PLAYER});
        return listener;
    }

    @Bean
    @StepScope
    @Qualifier("jobStartLoggerListener")
    public StepExecutionListener jobStartLoggerListener(@Value("#{jobParameters['uuid']}") String uuid) {
        return new StepExecutionListener() {
            @Override
            public void beforeStep(StepExecution stepExecution) {
                LOGGER.info("Job with uuid = " + uuid + " is started");
            }
        };
    }


    @Bean
    @Qualifier("teamMaxRatioPerformanceStep")
    public Step teamMaxRatioPerformanceStep(JobRepository jobRepository, PlatformTransactionManager transactionManager,
                                            @Qualifier("maxRatioPerformanceProcessor") ItemProcessor<AverageScoredTeam, TeamPerformance> maxRatioPerformanceProcessor,
                                            @Qualifier("maxHeaderWriter") FlatFileHeaderCallback maxHeaderWriter) {
        return new StepBuilder("teamMaxRatioPerformanceStep", jobRepository)
                // Read-and-write one-by-one
                .<AverageScoredTeam, TeamPerformance>chunk(1, transactionManager)
                // Reading from average scored team file
                .reader(averageScoredTeamReader())
                .processor(maxRatioPerformanceProcessor)
                .writer(new FlatFileItemWriterBuilder<TeamPerformance>()
                        .name("teamMaxRatioPerformanceWriter")
                        .resource(maxPerformanceRatioOutResource)
                        .delimited()
                        .delimiter(",")
                        .fieldExtractor(team -> new Object[] {team.getName(), team.getPerformance()})
                        .headerCallback(maxHeaderWriter)
                        .build())
                .build();
    }

    @Bean
    @Qualifier("teamMinRatioPerformanceStep")
    public Step teamMinRatioPerformanceStep(JobRepository jobRepository, PlatformTransactionManager transactionManager,
                                            @Qualifier("minRatioPerformanceProcessor") ItemProcessor<AverageScoredTeam, TeamPerformance> minRatioPerformanceProcessor,
                                            @Qualifier("minHeaderWriter") FlatFileHeaderCallback minHeaderWriter) {
        return new StepBuilder("teamMinRatioPerformanceStep", jobRepository)
                // Read-and-write one-by-one
                .<AverageScoredTeam, TeamPerformance>chunk(1, transactionManager)
                // Reading from average scored team file
                .reader(averageScoredTeamReader())
                .processor(minRatioPerformanceProcessor)
                .writer(new FlatFileItemWriterBuilder<TeamPerformance>()
                        .name("teamMinRatioPerformanceWriter")
                        .resource(minPerformanceRatioOutResource)
                        .delimited()
                        .delimiter(",")
                        .fieldExtractor(team -> new Object[] {team.getName(), team.getPerformance()})
                        .headerCallback(minHeaderWriter)
                        .build())
                .build();
    }

    // Create new instance of file item reader: defined as a separate method to be reused,
    // however not defined as a bean to have a separate instance per each step (executed in parallel)
    public ItemReader<AverageScoredTeam> averageScoredTeamReader() {
        return new FlatFileItemReaderBuilder<AverageScoredTeam>()
                .name("averageScoredTeamReader")
                .resource(outAvgResource)
                .lineTokenizer(new DelimitedLineTokenizer(","))
                .fieldSetMapper(fieldSet ->
                        new AverageScoredTeam(fieldSet.readString(0), fieldSet.readDouble(1)))
                .build();
    }

    @Bean
    @StepScope
    @Qualifier("maxRatioPerformanceProcessor")
    public ItemProcessor<AverageScoredTeam, TeamPerformance> maxRatioPerformanceProcessor(@Value("#{jobExecutionContext['max.score']}") double maxScore) {
        return item -> process(item, maxScore);
    }

    @Bean
    @StepScope
    @Qualifier("minRatioPerformanceProcessor")
    public ItemProcessor<AverageScoredTeam, TeamPerformance> minRatioPerformanceProcessor(@Value("#{jobExecutionContext['min.score']}") double minScore) {
        return item -> process(item, minScore);
    }

    // Method which is processing average scored item into the team performance given the baseline score
    // Performance is represented as "X%" string, where X = score * 100 / baseline, with up to 2 precision
    private static TeamPerformance process(AverageScoredTeam team, double baselineScore) {
        BigDecimal performance = BigDecimal.valueOf(team.getAverageScore())
                .multiply(new BigDecimal(100))
                .divide(BigDecimal.valueOf(baselineScore), 2, RoundingMode.HALF_UP);
        return new TeamPerformance(team.getName(), performance.toString() + "%");
    }

    @Bean
    @StepScope
    @Qualifier("maxHeaderWriter")
    public FlatFileHeaderCallback maxHeaderWriter(@Value("#{jobExecutionContext['max.score']}") double maxScore,
                                                  @Value("#{jobExecutionContext['max.player']}") String maxPlayerName) {
        return writer -> writeHeader(writer, maxPlayerName, maxScore);
    }

    @Bean
    @StepScope
    @Qualifier("minHeaderWriter")
    public FlatFileHeaderCallback minHeaderWriter(@Value("#{jobExecutionContext['min.score']}") double minScore,
                                                  @Value("#{jobExecutionContext['min.player']}") String minPlayerName) {
        return writer -> writeHeader(writer, minPlayerName, minScore);
    }


    // Writes the header to the team performance file, mentioning baseline value and author player
    private static void writeHeader(Writer writer, String name, double score) {
        try {
            writer.write("******************************************************\n");
            writer.write("Team performances below are calculated against " + score + " which was scored by " + name + "\n");
            writer.write("******************************************************\n");
        } catch (Exception e) {
            // Re-throw as unchecked
            throw new RuntimeException(e);
        }
    }

    @Bean
    @Qualifier("shellScriptStep")
    public Step shellScriptStep(PlatformTransactionManager transactionManager,
                                JobRepository jobRepository,
                                @Qualifier("shellScriptTasklet") Tasklet shellScriptTasklet) {
        return new StepBuilder("shellScriptStep", jobRepository)
                .tasklet(shellScriptTasklet, transactionManager)
                .build();
    }

    @Bean
    @StepScope
    @Qualifier("shellScriptTasklet")
    public Tasklet shellScriptTasklet(@Value("#{jobParameters['uuid']}") String uuid) {
        return (contribution, chunkContext) -> {
            CommandRunner commandRunner = new JvmCommandRunner();
            commandRunner.exec(new String[] {"bash", "-l", "-c", "touch " + uuid + ".resulted"},
                    new String[] {}, calculatedDirectoryResource.getFile());
            return RepeatStatus.FINISHED;
        };
    }

    @Bean
    @Qualifier("successLoggerStep")
    public Step successLoggerStep(PlatformTransactionManager transactionManager,
                                  JobRepository jobRepository,
                                  @Qualifier("successLoggerTasklet") Tasklet successLoggerTasklet) {
        return new StepBuilder("successLoggerStep", jobRepository)
                .tasklet(successLoggerTasklet, transactionManager)
                .build();
    }

    @Bean
    @StepScope
    @Qualifier("successLoggerTasklet")
    public Tasklet successLoggerTasklet(@Value("#{jobParameters['uuid']}") String uuid) {
        return (contribution, chunkContext) -> {
            LOGGER.info("Job with uuid = " + uuid + " is finished");
            return RepeatStatus.FINISHED;
        };
    }

    /* ******************************** Spring Batch Utilities are defined below ********************************** */

    /**
     * Since we would like to launch jobs asynchronously, we would like to create async job launcher,
     * since out-of-the-box Spring Batch job launcher is synchronous
     */
    @Bean
    @Qualifier("asyncJobLauncher")
    public JobLauncher asyncJobLauncher(JobRepository jobRepository) {
        TaskExecutorJobLauncher jobLauncher = new TaskExecutorJobLauncher();
        jobLauncher.setJobRepository(jobRepository);
        jobLauncher.setTaskExecutor(new SimpleAsyncTaskExecutor());
        return jobLauncher;
    }

    /**
     * Due to the fact we are not using standard Spring-expected naming and directory structure,
     * {@link org.springframework.batch.core.configuration.annotation.EnableBatchProcessing} is not able
     * to auto-create data source bean, so it's defined explicitly here
     */
    @Bean
    public DataSource dataSource(@Value("${db.url}") String url,
                                 @Value("${db.username}") String username,
                                 @Value("${db.password}") String password) {
        DriverManagerDataSource dataSource = new DriverManagerDataSource();
        dataSource.setDriverClassName("com.mysql.cj.jdbc.Driver");
        dataSource.setUrl(url);
        dataSource.setUsername(username);
        dataSource.setPassword(password);

        return dataSource;
    }


    /**
     * Due to the fact we are not using standard Spring-expected naming and directory structure,
     * {@link org.springframework.batch.core.configuration.annotation.EnableBatchProcessing} is not able
     * do detect whether the database should be initialized on start. So, we explicitly override it with
     * supplying {@link BatchProperties} bean defined below. In the configuration with correct naming and
     * directory setup, 'spring.batch.initialize-schema' property should make things work magically
     */
    @Bean
    public BatchDataSourceScriptDatabaseInitializer batchDataSourceInitializer(DataSource dataSource,
                                                                               BatchProperties properties) {
        return new BatchDataSourceScriptDatabaseInitializer(dataSource, properties.getJdbc());
    }

    /**
     * Due to the fact we are not using standard Spring-expected naming and directory structure,
     * {@link org.springframework.batch.core.configuration.annotation.EnableBatchProcessing} is not able
     * do detect whether the database should be initialized on start. So, we explicitly define the
     * {@link BatchProperties} bean with auto-wiring the configured value. In the configuration with correct
     * naming and directory setup, 'spring.batch.initialize-schema' property should make things work magically
     */
    @Bean
    public BatchProperties batchProperties(@Value("${batch.db.initialize-schema}") DatabaseInitializationMode initializationMode) {
        BatchProperties properties = new BatchProperties();
        properties.getJdbc().setInitializeSchema(initializationMode);
        return properties;
    }
}
