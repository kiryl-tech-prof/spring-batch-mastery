package org.example;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.job.AbstractJob;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.TaskExecutorJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.batch.item.database.PagingQueryProvider;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.database.builder.JdbcPagingItemReaderBuilder;
import org.springframework.batch.item.support.builder.SynchronizedItemStreamReaderBuilder;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.batch.BatchDataSourceScriptDatabaseInitializer;
import org.springframework.boot.autoconfigure.batch.BatchProperties;
import org.springframework.boot.sql.init.DatabaseInitializationMode;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.support.JdbcTransactionManager;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;


@Configuration
@EnableBatchProcessing
@PropertySource("classpath:db.properties")
@PropertySource("classpath:partitioning.properties")
public class UserActionJobConfiguration {

    private static final Logger LOGGER = LoggerFactory.getLogger(UserActionJobConfiguration.class);

    @Bean
    @Qualifier("simpleActionCalculationJob")
    public AbstractJob simpleActionCalculationJob(JobRepository jobRepository,
                                                  @Qualifier("simpleActionCalculationStep") Step simpleActionCalculationStep) {
        return (AbstractJob) new JobBuilder("simpleActionCalculationJob", jobRepository)
                .start(simpleActionCalculationStep)
                .build();
    }

    @Bean
    @Qualifier("multiThreadedActionCalculationJob")
    public Job multiThreadedActionCalculationJob(JobRepository jobRepository,
                                                 @Qualifier("multiThreadedActionCalculationStep") Step multiThreadedActionCalculationStep) {
        return new JobBuilder("multiThreadedActionCalculationJob", jobRepository)
                .start(multiThreadedActionCalculationStep)
                .build();
    }

    @Bean
    @Qualifier("partitionedLocalActionCalculationJob")
    public Job partitionedLocalActionCalculationJob(JobRepository jobRepository,
                                                    @Qualifier("partitionedLocalActionCalculationStep") Step partitionedLocalActionCalculationStep) {
        return new JobBuilder("partitionedLocalActionCalculationJob", jobRepository)
                .start(partitionedLocalActionCalculationStep)
                .build();
    }

    @Bean
    @Qualifier("partitionedRemoteActionCalculationJob")
    public Job partitionedRemoteActionCalculationJob(JobRepository jobRepository,
                                                     @Qualifier("partitionedRemoteActionCalculationStep") Step partitionedRemoteActionCalculationStep) {
        return new JobBuilder("partitionedRemoteActionCalculationJob", jobRepository)
                .start(partitionedRemoteActionCalculationStep)
                .build();
    }

    @Bean
    @Qualifier("partitionedRemoteActionCalculationStep")
    public Step partitionedRemoteActionCalculationStep(JobRepository jobRepository, JobExplorer jobExplorer,
                                                       @Qualifier("simpleActionCalculationStep") Step simpleActionCalculationStep,
                                                       PartitioningConfig partitioningConfig) {
        return new StepBuilder("partitionedRemoteActionCalculationStep", jobRepository)
                .partitioner("simpleActionCalculationStep", new SessionActionPartitioner())
                .partitionHandler(new HttpRequestPartitionHandler(simpleActionCalculationStep, partitioningConfig,
                                               30000, jobRepository, jobExplorer))
                .build();
    }

    @Bean
    @Qualifier("partitionedLocalActionCalculationStep")
    public Step partitionedLocalActionCalculationStep(JobRepository jobRepository,
                                                      @Qualifier("simpleActionCalculationStep") Step simpleActionCalculationStep) {
        return new StepBuilder("partitionedLocalActionCalculationStep", jobRepository)
                .partitioner("simpleActionCalculationStep", new SessionActionPartitioner())
                .step(simpleActionCalculationStep)
                .taskExecutor(new SimpleAsyncTaskExecutor())
                .gridSize(3) // Hard-code 3 threads to partition data handling
                .build();
    }

    @Bean
    @Qualifier("multiThreadedActionCalculationStep")
    public Step multiThreadedActionCalculationStep(JobRepository jobRepository, PlatformTransactionManager transactionManager,
                                                   @Qualifier("sessionActionReader") ItemStreamReader<SessionAction> sessionActionReader,
                                                   @Qualifier("sourceDataSource") DataSource sourceDataSource,
                                                   @Qualifier("multiThreadStepExecutor") TaskExecutor multiThreadStepExecutor) {
        return new StepBuilder("multiThreadedActionCalculationStep", jobRepository)
                // Write in batches of size 5
                .<SessionAction, UserScoreUpdate>chunk(5, transactionManager)
                // Read in pages of size 5 using synchronized reader
                // to mitigate thread safety problems while using shared reader between threads
                .reader(new SynchronizedItemStreamReaderBuilder<SessionAction>()
                        .delegate(sessionActionReader)
                        .build())
                // Convert items into user score update objects used to update with (score = score * a + b) idea
                .processor(getSessionActionProcessor())
                // Write into the database using the upsert capabilities; ideally, we should also wrap the writer
                // into synchronized writer to avoid deadlock issues with Postgres. However, we are not doing it,
                // since this step is provided for demonstration purposes anyway and will produce wrong results
                // because of strict order guarantee requirements because of the problem definition
                .writer(new JdbcBatchItemWriterBuilder<UserScoreUpdate>()
                        .dataSource(sourceDataSource)
                        .itemPreparedStatementSetter(SourceDatabaseUtils.UPDATE_USER_SCORE_PARAMETER_SETTER)
                        .sql(SourceDatabaseUtils.constructUpdateUserScoreQuery(UserScoreUpdate.USER_SCORE_TABLE_NAME))
                        .build())
                .listener(beforeStepLoggerListener())
                .taskExecutor(multiThreadStepExecutor)
                .build();
    }

    @Bean
    @Qualifier("multiThreadStepExecutor")
    public TaskExecutor multiThreadStepExecutor() {
        ThreadPoolTaskExecutor threadPoolExecutor = new ThreadPoolTaskExecutor();
        threadPoolExecutor.setCorePoolSize(3);
        return threadPoolExecutor;
    }

    @Bean
    @Qualifier("simpleActionCalculationStep")
    public Step simpleActionCalculationStep(JobRepository jobRepository, PlatformTransactionManager transactionManager,
                                            @Qualifier("sessionActionReader") ItemReader<SessionAction> sessionActionReader,
                                            @Qualifier("sourceDataSource") DataSource sourceDataSource) {
        return new StepBuilder("simpleActionCalculationStep", jobRepository)
                // Write in batches of size 5
                .<SessionAction, UserScoreUpdate>chunk(5, transactionManager)
                // Read also in pages of size 5 (configured in reader's bean definition)
                .reader(sessionActionReader)
                // Convert items into user score update objects used to update with (score = score * a + b) idea
                .processor(getSessionActionProcessor())
                // Write into the database using the upsert capabilities
                .writer(new JdbcBatchItemWriterBuilder<UserScoreUpdate>()
                        .dataSource(sourceDataSource)
                        .itemPreparedStatementSetter(SourceDatabaseUtils.UPDATE_USER_SCORE_PARAMETER_SETTER)
                        .sql(SourceDatabaseUtils.constructUpdateUserScoreQuery(UserScoreUpdate.USER_SCORE_TABLE_NAME))
                        .build())
                .listener(beforeStepLoggerListener())
                .build();
    }

    // Processor to process single session action item
    private static ItemProcessor<SessionAction, UserScoreUpdate> getSessionActionProcessor() {
        return sessionAction -> {
            if (SourceDatabaseUtils.PLUS_TYPE.equals(sessionAction.getActionType())) {
                return new UserScoreUpdate(sessionAction.getUserId(), sessionAction.getAmount(), 1d);
            } else if (SourceDatabaseUtils.MULTI_TYPE.equals(sessionAction.getActionType())) {
                return new UserScoreUpdate(sessionAction.getUserId(), 0d, sessionAction.getAmount());
            } else {
                throw new RuntimeException("Unknown session action record type: " + sessionAction.getActionType());
            }
        };
    }

    // Step execution listener that logs information about step and environment (thread) right before the start of the execution
    private static StepExecutionListener beforeStepLoggerListener() {
        return new StepExecutionListener() {
            @Override
            public void beforeStep(StepExecution stepExecution) {
                int partitionCount = stepExecution.getExecutionContext().getInt(SessionActionPartitioner.PARTITION_COUNT, -1);
                int partitionIndex = stepExecution.getExecutionContext().getInt(SessionActionPartitioner.PARTITION_INDEX, -1);
                if (partitionIndex == -1 || partitionCount == -1) {
                    LOGGER.info("Calculation step is about to start handling all session action records");
                } else {
                    String threadName = Thread.currentThread().getName();
                    LOGGER.info("Calculation step is about to start handling partition " + partitionIndex
                            + " out of total " + partitionCount + " partitions in the thread -> " + threadName);
                }
            }
        };
    }

    @Bean
    @StepScope // Reader is step scope to auto-wire partition properties from the step execution context
    @Qualifier("sessionActionReader")
    public ItemStreamReader<SessionAction> sessionActionReader(@Qualifier("sourceDataSource") DataSource sourceDataSource,
                                                               @Value("#{stepExecutionContext['partitionCount']}") Integer partitionCount,
                                                               @Value("#{stepExecutionContext['partitionIndex']}") Integer partitionIndex) {
        // Select all in case no partition properties passed; select partition-specific records otherwise
        PagingQueryProvider queryProvider = (partitionCount == null || partitionIndex == null)
                ? SourceDatabaseUtils.selectAllSessionActionsProvider(SessionAction.SESSION_ACTION_TABLE_NAME)
                : SourceDatabaseUtils
                    .selectPartitionOfSessionActionsProvider(SessionAction.SESSION_ACTION_TABLE_NAME, partitionCount, partitionIndex);
        return new JdbcPagingItemReaderBuilder<SessionAction>()
                .name("sessionActionReader")
                .dataSource(sourceDataSource)
                .queryProvider(queryProvider)
                .rowMapper(SourceDatabaseUtils.getSessionActionMapper())
                // Querying the database in pages of 5
                .pageSize(5)
                .build();
    }

    @Bean
    public PartitioningConfig partitioningConfig(@Value("${worker.server.base.urls}") String workerServerBaseUrls) {
        return new PartitioningConfig(workerServerBaseUrls);
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

    // Transaction manager for source data source, to control boundaries of storing the data in Postgresql
    @Bean
    public PlatformTransactionManager transactionManager(@Qualifier("sourceDataSource") DataSource sourceDataSource) {
        JdbcTransactionManager transactionManager = new JdbcTransactionManager();
        transactionManager.setDataSource(sourceDataSource);
        return transactionManager;
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
