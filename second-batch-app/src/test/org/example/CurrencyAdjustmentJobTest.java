package org.example;

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
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import javax.sql.DataSource;
import java.math.RoundingMode;
import java.util.List;
import java.util.Random;
import java.util.UUID;


// End-to-end test for currency adjustment job
@SpringBatchTest
@SpringJUnitConfig(BankTransactionAnalysisConfiguration.class)
@TestPropertySource("classpath:test_source.properties") // Supply test config to override the database name used for tests
public class CurrencyAdjustmentJobTest {

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;

    @Autowired
    @Qualifier("currencyAdjustmentJob")
    private Job currencyAdjustmentJob;

    @Value("${currency.adjustment.rate}")
    private double currencyAdjustmentRate;

    private JdbcTemplate jdbcTemplate;

    @Autowired
    public void setDataSource(@Qualifier("sourceDataSource") DataSource dataSource) {
        this.jdbcTemplate = new JdbcTemplate(dataSource);
    }

    @BeforeEach // Before each @Test (in this class, it's only one test), initialize the database
    public void initDatabase() {
        SourceManagementUtils.initializeEmptyDatabase(jdbcTemplate);
    }

    @Test
    public void testCurrencyAdjustmentJob() throws Exception {
        Random random = new Random();
        String[] merchants = new String[] {UUID.randomUUID().toString()};

        BankTransaction[] generatedTransactions = new BankTransaction[] {
                GenerateSourceDatabase.generateRecord(random, merchants),
                GenerateSourceDatabase.generateRecord(random, merchants),
                GenerateSourceDatabase.generateRecord(random, merchants)
        };

        for (BankTransaction generatedTransaction : generatedTransactions) {
            SourceManagementUtils.insertBankTransaction(generatedTransaction, jdbcTemplate);
        }

        // Configure job launcher to run the job
        jobLauncherTestUtils.setJob(currencyAdjustmentJob);
        // Launch the currency adjustment job
        JobExecution jobExecution = jobLauncherTestUtils.launchJob();

        // Verify that job executed successfully
        Assertions.assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());

        List<BankTransaction> dbTransactions = jdbcTemplate
                .query(BankTransaction.SELECT_ALL_QUERY, BankTransaction.ROW_MAPPER);

        for (BankTransaction dbTransaction : dbTransactions) {
            BankTransaction generatedTransaction = generatedTransactions[(int) dbTransaction.getId() - 1];
            // We check whether each transaction amount was adjusted
            Assertions.assertEquals(dbTransaction.getAmount()
                            .divide(generatedTransaction.getAmount(), RoundingMode.HALF_UP)
                            .doubleValue(),
                                    // Tiny delta due to precision of floating numbers in computers
                                    currencyAdjustmentRate, 0.0000001);
        }
    }
}
