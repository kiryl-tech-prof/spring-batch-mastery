package org.example;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.batch.test.context.SpringBatchTest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.Resource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import javax.sql.DataSource;
import java.math.BigDecimal;
import java.util.UUID;


// End-to-end test for the step aggregating transactions by day & month
@SpringBatchTest
@SpringJUnitConfig(BankTransactionAnalysisConfiguration.class)
@TestPropertySource("classpath:test_source.properties") // Supply test config to override the database name used for tests
public class AggregateByDayStepTest {

    @Value("classpath:test/expected_day_aggregation.json")
    private Resource expectedAggregationJsonResource;

    @Value("file:daily_balance.json") // Output file (result of the step execution) location
    private Resource dailyBalanceJsonResource;

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;

    @Autowired
    @Qualifier("bankTransactionAnalysisJob")
    private Job bankTransactionAnalysisJob;

    private JdbcTemplate jdbcTemplate;

    @Autowired
    public void setDataSource(@Qualifier("sourceDataSource") DataSource dataSource) {
        this.jdbcTemplate = new JdbcTemplate(dataSource);
    }

    @BeforeEach // Before each @Test (though only a single test in this class), initialize the database
    public void initDatabase() {
        SourceManagementUtils.initializeEmptyDatabase(jdbcTemplate);
    }

    // Verify that aggregated by day step finishes correctly and generated the expected data
    @Test
    public void testAggregateByDayStep() throws Exception {
        BankTransaction[] generatedTransactions = new BankTransaction[] {
                new BankTransaction(-1, 1, 13, 0, 0, new BigDecimal("2.5"), UUID.randomUUID().toString()),
                new BankTransaction(-1, 1, 13, 0, 0, new BigDecimal("2"), UUID.randomUUID().toString()),
                new BankTransaction(-1, 2, 17, 0, 0, new BigDecimal("8"), UUID.randomUUID().toString()),
                new BankTransaction(-1, 2, 17, 0, 0, new BigDecimal("6.11"), UUID.randomUUID().toString()),
                new BankTransaction(-1, 2, 29, 0, 0, new BigDecimal("-29.29"), UUID.randomUUID().toString())
        };

        // Insert test bank transactions
        for (BankTransaction generatedTransaction : generatedTransactions) {
            SourceManagementUtils.insertBankTransaction(generatedTransaction, jdbcTemplate);
        }

        // Configure job launcher to run the job
        jobLauncherTestUtils.setJob(bankTransactionAnalysisJob);
        JobExecution jobExecution = jobLauncherTestUtils.launchStep("aggregate-by-day");

        // Verify that step executed successfully
        Assertions.assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());

        // Verify that the contents of the file are equal line-by-line
        /*
        AssertFile might be used, but it's deprecated

        AssertFile.assertFileEquals(expectedAggregationJsonResource, dailyBalanceJsonResource);
         */
        Assertions.assertTrue(jsonFilesEqualContent(expectedAggregationJsonResource, dailyBalanceJsonResource));
    }

    // Utility method leveraging Jackson library to compare contents of json files
    private boolean jsonFilesEqualContent(Resource test, Resource expected) throws Exception {
        ObjectMapper mapper = new ObjectMapper();

        JsonNode treeTest = mapper.readTree(test.getFile());
        JsonNode treeExpected = mapper.readTree(expected.getFile());

        return treeTest.equals(treeExpected);
    }
}
