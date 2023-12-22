package org.example;

import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.ItemReadListener;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.configuration.support.DefaultBatchConfiguration;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder;
import org.springframework.batch.item.database.builder.JdbcPagingItemReaderBuilder;
import org.springframework.batch.item.json.JacksonJsonObjectMarshaller;
import org.springframework.batch.item.json.builder.JsonFileItemWriterBuilder;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.batch.BatchDataSourceScriptDatabaseInitializer;
import org.springframework.boot.autoconfigure.batch.BatchProperties;
import org.springframework.boot.sql.init.DatabaseInitializationMode;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.io.WritableResource;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.support.JdbcTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.math.BigDecimal;
import java.math.RoundingMode;


@Configuration
@PropertySource("classpath:job_repo.properties")
@PropertySource("classpath:currency_adjustment.properties")
@Import(SourceConfiguration.class) // Include input source configuration
public class BankTransactionAnalysisConfiguration extends DefaultBatchConfiguration {

    // Constants for exit statuses of the fill balance step
    public static final String POSITIVE = "POSITIVE";
    public static final String NEGATIVE = "NEGATIVE";

    @Value("file:merchant_month.json")
    private WritableResource merchantMonthlyBalanceJsonResource;

    @Value("file:daily_balance.json")
    private WritableResource dailyBalanceJsonResource;

    @Bean
    @Qualifier("bankTransactionAnalysisJob")
    public Job bankTransactionAnalysisJob(JobRepository jobRepository,
                                          @Qualifier("fillBalanceStep") Step fillBalanceStep,
                                          @Qualifier("aggregateByMerchantMonthlyStep") Step aggregateByMerchantMonthlyStep,
                                          @Qualifier("aggregateByDayStep") Step aggregateByDayStep) {
        return new JobBuilder("bankTransactionAnalysisJob", jobRepository)
                // Always start from filling the balance
                .start(fillBalanceStep)
                // 'on' is referring to previously mentioned step, fillBalanceStep, saying: if exit status is positive,
                // then go to aggregateByMerchantMonthlyStep
                .on(POSITIVE).to(aggregateByMerchantMonthlyStep)
                // Since we finished with one branch, here we mention explicitly: if fillBalanceStep exit status is negative,
                // then go to aggregateByDayStep
                .from(fillBalanceStep).on(NEGATIVE).to(aggregateByDayStep)
                // The last possibility is finishing some other way, most likely with error; terminate in this case
                .from(fillBalanceStep).on("*").end()
                .end()
                .build();
    }

    @Bean
    @Qualifier("fillBalanceStep")
    public Step fillBalanceStep(JobRepository jobRepository, PlatformTransactionManager transactionManager,
                                @Qualifier("sourceDataSource") DataSource sourceDataSource) {
        FillBalanceProcessor processor = new FillBalanceProcessor();
        return new StepBuilder("fill-balance", jobRepository)
                // Writing in chunks of size 10
                .<BankTransaction, BalanceUpdate>chunk(10, transactionManager)
                // Reading from source db using cursor-based technique
                .reader(new JdbcCursorItemReaderBuilder<BankTransaction>()
                            .dataSource(sourceDataSource)
                            .name("bankTransactionReader")
                            .sql(BankTransaction.SELECT_ALL_QUERY)
                            .rowMapper(BankTransaction.ROW_MAPPER)
                            .build())
                // Using instance of the processor, such that step execution is set properly
                .processor(processor)
                // Writer needs to update record's information by writing 'balance' column
                .writer(new JdbcBatchItemWriterBuilder<BalanceUpdate>()
                            .dataSource(sourceDataSource)
                            .itemPreparedStatementSetter((item, ps) -> {
                                ps.setBigDecimal(1, item.getBalance());
                                ps.setLong(2, item.getId());
                            })
                            .sql("update bank_transaction_yearly set balance = ? where id = ?")
                            .build())
                // Step execution listener to alter schema before step, and plug and unplug step execution for processor
                .listener(new StepExecutionListener() {
                    @Override
                    public void beforeStep(StepExecution stepExecution) {
                        SourceManagementUtils.addBalanceColumn(sourceDataSource);
                        processor.setStepExecution(stepExecution);
                    }

                    @Override
                    public ExitStatus afterStep(StepExecution stepExecution) {
                        double totalBalance = processor.getLatestTransactionBalance();
                        processor.setStepExecution(null); // Clear step execution when step is executed
                        return new ExitStatus(totalBalance >= 0 ? POSITIVE : NEGATIVE);
                    }
                })
                // Always run step, regardless of whether same parameters step was completed
                .allowStartIfComplete(true)
                .build();
    }


    @Bean
    @Qualifier("aggregateByMerchantMonthlyStep")
    public Step aggregateByMerchantMonthlyStep(JobRepository jobRepository,
                                               PlatformTransactionManager transactionManager,
                                               @Qualifier("merchantMonthAggregationReader") ItemReader<MerchantMonthBalance> merchantMonthAggregationReader) {
        return new StepBuilder("aggregate-by-merchant-monthly", jobRepository)
                // Writing in chunks of size 10
                .<MerchantMonthBalance, MerchantMonthBalance>chunk(10, transactionManager)
                // Supplying paging reader defined as a bean, opposing to instance, to work-around Spring Batch flaw;
                // if not defined as a bean, NPE will be raised since some resources will not be initialized in
                // .afterPropertiesSet() method, which is called by Spring post-bean-initialization.
                // Potential, though not clean, alternative is to call .afterPropertiesSet() explicitly in code,
                // but it is preferred to have a proper bean definition instead
                .reader(merchantMonthAggregationReader)
                // Writing to JSON file without any processing
                .writer(new JsonFileItemWriterBuilder<MerchantMonthBalance>()
                        .jsonObjectMarshaller(new JacksonJsonObjectMarshaller<>())
                        .resource(merchantMonthlyBalanceJsonResource)
                        .name("merchantMonthAggregationWriter")
                        .build())
                // Always run step, regardless of whether same parameters step was completed
                .allowStartIfComplete(true)
                .build();
    }

    @Bean
    @Qualifier("aggregateByDayStep")
    public Step aggregateByDayStep(JobRepository jobRepository,
                                   PlatformTransactionManager transactionManager,
                                   @Qualifier("dailyBalanceAggregationReader") ItemReader<DailyBalance> dailyBalanceAggregationReader) {
        return new StepBuilder("aggregate-by-day", jobRepository)
                // Writing in chunks of size 10
                .<DailyBalance, DailyBalance>chunk(10, transactionManager)
                // Supplying paging reader defined as a bean, opposing to instance, to work-around Spring Batch flaw;
                // if not defined as a bean, NPE will be raised since some resources will not be initialized in
                // .afterPropertiesSet() method, which is called by Spring post-bean-initialization.
                // Potential, though not clean, alternative is to call .afterPropertiesSet() explicitly in code,
                // but it is preferred to have a proper bean definition instead
                .reader(dailyBalanceAggregationReader)
                // Writing to JSON file without any processing
                .writer(new JsonFileItemWriterBuilder<DailyBalance>()
                        .jsonObjectMarshaller(new JacksonJsonObjectMarshaller<>())
                        .resource(dailyBalanceJsonResource)
                        .name("dailyBalanceAggregationWriter")
                        .build())
                // Always run step, regardless of whether same parameters step was completed
                .allowStartIfComplete(true)
                .build();
    }

    @Bean
    @Qualifier("merchantMonthAggregationReader")
    public ItemReader<MerchantMonthBalance> merchantMonthAggregationReader(@Qualifier("sourceDataSource") DataSource sourceDataSource) {
        // Paging-style reader
        return new JdbcPagingItemReaderBuilder<MerchantMonthBalance>()
                .name("merchantMonthAggregationReader")
                .dataSource(sourceDataSource)
                .queryProvider(MerchantMonthBalance.getQueryProvider())
                .rowMapper(MerchantMonthBalance.ROW_MAPPER)
                // Querying the database in chinks of 5
                .pageSize(5)
                .build();
    }

    @Bean
    @Qualifier("dailyBalanceAggregationReader")
    public ItemReader<DailyBalance> dailyBalanceAggregationReader(@Qualifier("sourceDataSource") DataSource sourceDataSource) {
        // Paging-style reader
        return new JdbcPagingItemReaderBuilder<DailyBalance>()
                .name("dailyBalanceAggregationReader")
                .dataSource(sourceDataSource)
                .queryProvider(DailyBalance.getQueryProvider())
                .rowMapper(DailyBalance.ROW_MAPPER)
                // Querying the database in chinks of 5
                .pageSize(5)
                .build( );
    }

    /* ******************************* Currency adjustment job is defined below *********************************** */

    // Domain entity representing one transaction adjustment
    private static class CurrencyAdjustment {
        long id;
        BigDecimal adjustedAmount;
    }

    @Bean
    @Qualifier("currencyAdjustmentJob")
    public Job currencyAdjustmentJob(JobRepository jobRepository,
                                     @Qualifier("currencyAdjustmentStep") Step currencyAdjustmentStep) {
        return new JobBuilder("currencyAdjustmentJob", jobRepository)
                .start(currencyAdjustmentStep)
                .build();
    }

    @Bean
    @Qualifier("currencyAdjustmentStep")
    public Step currencyAdjustmentStep(JobRepository jobRepository, PlatformTransactionManager transactionManager,
                                       @Qualifier("sourceDataSource") DataSource sourceDataSource,
                                       @Value("${currency.adjustment.rate}") double rate,
                                       @Value("${currency.adjustment.disallowed.merchant}") String disallowedMerchant) {
        return new StepBuilder("currency-adjustment", jobRepository)
                // Read & write one-by-one here
                .<BankTransaction, CurrencyAdjustment>chunk(1, transactionManager)
                .reader(new JdbcCursorItemReaderBuilder<BankTransaction>()
                        .dataSource(sourceDataSource)
                        .name("bankTransactionReader")
                        // Take flag into account
                        .sql(BankTransaction.SELECT_ALL_QUERY + " where adjusted = false")
                        .rowMapper(BankTransaction.ROW_MAPPER)
                        .saveState(false)
                        .build())
                // Calculate the adjustment: multiply the amount to rate
                .processor(item -> {
                    CurrencyAdjustment adjustment = new CurrencyAdjustment();
                    adjustment.id = item.getId();
                    adjustment.adjustedAmount = item.getAmount()
                            .multiply(BigDecimal.valueOf(rate))
                            .setScale(2, RoundingMode.HALF_UP);
                    return adjustment;
                })
                .writer(new JdbcBatchItemWriterBuilder<CurrencyAdjustment>()
                        .dataSource(sourceDataSource)
                        .itemPreparedStatementSetter((item, ps) -> {
                            ps.setBigDecimal(1, item.adjustedAmount);
                            ps.setBoolean(2, true); // Flag is set to true now
                            ps.setLong(3, item.id);
                        })
                        .sql("update bank_transaction_yearly set amount = ?, adjusted = ? where id = ?")
                        .build())
                .listener(new StepExecutionListener() {
                    @Override
                    public void beforeStep(StepExecution stepExecution) {
                        // Before executing the step, add boolean column (if needed)
                        SourceManagementUtils.addAdjustedColumn(sourceDataSource);
                    }
                })
                .listener(new ItemReadListener<>() {
                    @Override // After each read, check whether merchant is the disallowed merchant
                    public void afterRead(BankTransaction item) {
                        if (disallowedMerchant.equals(item.getMerchant())) {
                            throw new RuntimeException("Disallowed merchant!");
                        }
                    }
                })
                // Always run step, regardless of whether same parameters step was completed
                .allowStartIfComplete(true)
                .build();
    }


    /* ******************************** Spring Batch Utilities are defined below ********************************** */

    /**
     * Due to usage of {@link DefaultBatchConfiguration}, dataSource need to be manually created.
     * When {@link org.springframework.batch.core.configuration.annotation.EnableBatchProcessing} is used,
     * it would have been auto-created
     */
    @Bean
    @Qualifier("dataSource") // Job repository data source should be named 'dataSource' as per DefaultBatchConfiguration
    public DataSource dataSource(@Value("${db.job.repo.url}") String url,
                                 @Value("${db.job.repo.username}") String username,
                                 @Value("${db.job.repo.password}") String password) {
        DriverManagerDataSource dataSource = new DriverManagerDataSource();
        dataSource.setDriverClassName("com.mysql.cj.jdbc.Driver");
        dataSource.setUrl(url);
        dataSource.setUsername(username);
        dataSource.setPassword(password);

        return dataSource;
    }

    // Transaction manager for data source, to control boundaries of storing the data in Postgresql
    // Should be named 'transactionManager' as per DefaultBatchConfiguration
    @Bean
    public PlatformTransactionManager transactionManager(@Qualifier("dataSource") DataSource dataSource) {
        JdbcTransactionManager transactionManager = new JdbcTransactionManager();
        transactionManager.setDataSource(dataSource);
        return transactionManager;
    }

    /**
     * Due to usage of {@link DefaultBatchConfiguration}, db initializer need to defined in order for Spring Batch
     * to consider initializing the schema on the first usage. In case of
     * {@link org.springframework.batch.core.configuration.annotation.EnableBatchProcessing} usage, it would have
     * been resolved with 'spring.batch.initialize-schema' property
     */
    @Bean
    public BatchDataSourceScriptDatabaseInitializer batchDataSourceInitializer(@Qualifier("dataSource") DataSource dataSource,
                                                                               BatchProperties properties) {
        return new BatchDataSourceScriptDatabaseInitializer(dataSource, properties.getJdbc());
    }

    /**
     * Due to usage of {@link DefaultBatchConfiguration}, we need to explicitly (programmatically) set initializeSchema
     * mode, and we are taking this parameter from the configuration wile, defined at {@link PropertySource} on class level;
     * In case we'd use {@link org.springframework.batch.core.configuration.annotation.EnableBatchProcessing}, having
     * 'spring.batch.initialize-schema' property would be enough
     */
    @Bean
    public BatchProperties batchProperties(@Value("${batch.db.initialize-schema}") DatabaseInitializationMode initializationMode) {
        BatchProperties properties = new BatchProperties();
        properties.getJdbc().setInitializeSchema(initializationMode);
        return properties;
    }
}
