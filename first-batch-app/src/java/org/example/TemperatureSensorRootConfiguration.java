package org.example;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.support.DefaultBatchConfiguration;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.batch.item.xml.builder.StaxEventItemReaderBuilder;
import org.springframework.batch.item.xml.builder.StaxEventItemWriterBuilder;
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
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.support.JdbcTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;


// Root configuration class for temperature sensors job
@Configuration
@PropertySource("classpath:db.properties")
public class TemperatureSensorRootConfiguration extends DefaultBatchConfiguration {

    @Value("classpath:input/HTE2NP.txt")
    private Resource rawDailyInputResource;

    @Value("file:HTE2NP.xml")
    private WritableResource aggregatedDailyOutputXmlResource;

    @Value("file:HTE2NP-anomalies.csv")
    private WritableResource anomalyDataResource;

    @Bean
    public Job temperatureSensorJob(JobRepository jobRepository,
                                    @Qualifier("aggregateSensorStep") Step aggregateSensorStep,
                                    @Qualifier("reportAnomaliesStep") Step reportAnomaliesStep) {
        return new JobBuilder("temperatureSensorJob", jobRepository)
                .start(aggregateSensorStep)
                .next(reportAnomaliesStep)
                .build();
    }

    @Bean
    @Qualifier("aggregateSensorStep")
    public Step aggregateSensorStep(JobRepository jobRepository, PlatformTransactionManager transactionManager) {
        return new StepBuilder("aggregate-sensor", jobRepository)
                // Reading in chunks of size 1, item-by-item
                .<DailySensorData, DailyAggregatedSensorData>chunk(1, transactionManager)
                // Reading from text file supplying mapper behavior
                .reader(new FlatFileItemReaderBuilder<DailySensorData>()
                        .name("dailySensorDataReader")
                        .resource(rawDailyInputResource)
                        .lineMapper(new SensorDataTextMapper())
                        .build())
                .processor(new RawToAggregateSensorDataProcessor())
                // Writing to XML file supplying marshaller (mapping elements / field names to type info)
                .writer(new StaxEventItemWriterBuilder<DailyAggregatedSensorData>()
                        .name("dailyAggregatedSensorDataWriter")
                        .marshaller(DailyAggregatedSensorData.getMarshaller())
                        .resource(aggregatedDailyOutputXmlResource)
                        .rootTagName("data")
                        .overwriteOutput(true)
                        .build())
                .build();
    }

    @Bean
    @Qualifier("reportAnomaliesStep")
    public Step reportAnomaliesStep(JobRepository jobRepository, PlatformTransactionManager transactionManager) {
        return new StepBuilder("report-anomalies", jobRepository)
                // Reading in chunks of size 1, item-by-item
                .<DailyAggregatedSensorData, DataAnomaly>chunk(1, transactionManager)
                // Reading from XML file re-using the same marshaller as for writing
                .reader(new StaxEventItemReaderBuilder<DailyAggregatedSensorData>()
                        .name("dailyAggregatedSensorDataReader")
                        .unmarshaller(DailyAggregatedSensorData.getMarshaller())
                        .resource(aggregatedDailyOutputXmlResource)
                        .addFragmentRootElements(DailyAggregatedSensorData.ITEM_ROOT_ELEMENT_NAME)
                        .build()
                )
                .processor(new SensorDataAnomalyProcessor())
                // Writing comma-delimited CSV format
                .writer(new FlatFileItemWriterBuilder<DataAnomaly>()
                        .name("dataAnomalyWriter")
                        .resource(anomalyDataResource)
                        .delimited()
                        .delimiter(",")
                        .names(new String[] {"date", "type", "value"})
                        .build()
                )
                .build();
    }

    /* ******************************** Spring Batch Utilities are defined below ********************************** */

    /**
     * Due to usage of {@link DefaultBatchConfiguration}, dataSource need to be manually created.
     * When {@link org.springframework.batch.core.configuration.annotation.EnableBatchProcessing} is used,
     * it would have been auto-created
     */
    @Bean
    public DataSource dataSource(@Value("${db.driverClassName}") String driverClassName,
                                 @Value("${db.url}") String url,
                                 @Value("${db.username}") String username,
                                 @Value("${db.password}") String password) {
        DriverManagerDataSource dataSource = new DriverManagerDataSource();
        dataSource.setDriverClassName(driverClassName);
        dataSource.setUrl(url);
        dataSource.setUsername(username);
        dataSource.setPassword(password);

        return dataSource;
    }

    // Simplest possible transaction manager configuration to let Spring Batch persist metadata about job / step completions
    @Bean
    public PlatformTransactionManager transactionManager(DataSource dataSource) {
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
    public BatchDataSourceScriptDatabaseInitializer batchDataSourceInitializer(DataSource dataSource,
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
