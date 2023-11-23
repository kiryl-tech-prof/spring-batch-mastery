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
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import javax.sql.DataSource;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.UUID;


// End-to-end test for the step filling the balance
@SpringBatchTest
@SpringJUnitConfig(BankTransactionAnalysisConfiguration.class)
@TestPropertySource("classpath:test_source.properties") // Supply test config to override the database name used for tests
public class FillBalanceStepTest {

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

    @BeforeEach // Before each @Test, initialize the database
    public void initDatabase() {
        SourceManagementUtils.initializeEmptyDatabase(jdbcTemplate);
    }

    // Test checking that balance field is filled and that it is filled correctly
    @Test
    public void testFillTheBalanceStep() {
        Random random = new Random();
        int transactionCount = random.nextInt(200) + 1; // Testing for at least one transaction
        List<BankTransaction> generatedTransactions = new ArrayList<>(transactionCount);

        // Generate test bank transactions
        for (int i = 0; i < transactionCount; i++) {
            generatedTransactions.add(GenerateSourceDatabase.generateRecord(random, new String[] {UUID.randomUUID().toString()}));
        }

        // Insert test bank transactions
        for (BankTransaction generatedTransaction : generatedTransactions) {
            SourceManagementUtils.insertBankTransaction(generatedTransaction, jdbcTemplate);
        }

        // Configure job launcher to run the job
        jobLauncherTestUtils.setJob(bankTransactionAnalysisJob);
        JobExecution jobExecution = jobLauncherTestUtils.launchStep("fill-balance");

        // Verify that step executed successfully
        Assertions.assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());

        // Query for all balances for all records in the respective order
        List<BigDecimal> dbBalanceList = jdbcTemplate
                .query("select balance from bank_transaction_yearly order by id",
                       (rs, rowNum) -> rs.getBigDecimal("balance"));

        Assertions.assertEquals(generatedTransactions.size(), dbBalanceList.size());

        BigDecimal runningBalance = BigDecimal.ZERO;
        // We are using iterators to avoid accessing elements of list by index
        Iterator<BankTransaction> transactionIterator = generatedTransactions.iterator();
        Iterator<BigDecimal> dbBalanceIterator = dbBalanceList.iterator();

        while (transactionIterator.hasNext()) {
            BigDecimal transactionAmount = transactionIterator.next().getAmount();
            runningBalance = runningBalance.add(transactionAmount);
            // Check whether balance is correct for each db record
            Assertions.assertEquals(runningBalance, dbBalanceIterator.next());
        }

        String expectedExitCode = runningBalance.compareTo(BigDecimal.ZERO) < 0 ?
                BankTransactionAnalysisConfiguration.NEGATIVE :
                BankTransactionAnalysisConfiguration.POSITIVE;

        Assertions.assertEquals(jobExecution.getExitStatus().getExitCode(), expectedExitCode);
    }

    // Test that fill-the-balance step is finishing correctly even with empty table (0 records)
    @Test
    public void testFillTheBalanceStepWithEmptyTable() {
        // Configure job launcher to run the job
        jobLauncherTestUtils.setJob(bankTransactionAnalysisJob);
        JobExecution jobExecution = jobLauncherTestUtils.launchStep("fill-balance");

        // Verify that the table still has 0 records
        List<BankTransaction> dbTransactions = jdbcTemplate
                .query(BankTransaction.SELECT_ALL_QUERY, BankTransaction.ROW_MAPPER);
        Assertions.assertEquals(dbTransactions.size(), 0);

        // Verify that job executed successfully
        Assertions.assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());

        // Verify that exit code is positive (corner case scenario)
        Assertions.assertEquals(jobExecution.getExitStatus().getExitCode(), BankTransactionAnalysisConfiguration.POSITIVE);
    }
}
