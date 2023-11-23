package org.example;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.test.MetaDataInstanceFactory;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Random;
import java.util.UUID;


// Unit test for FillBalanceProcessor with mocking implemented using both Spring Batch and Mockito
public class FillBalanceProcessorTest {

    private static final Random RANDOM = new Random();

    @Test
    public void testProcessorWithMetadataInstanceFactory() throws Exception {
        double balanceSoFar = RANDOM.nextDouble();

        // This part is mocking and uses MetaDataInstanceFactory
        StepExecution stepExecution = MetaDataInstanceFactory.createStepExecution();
        ExecutionContext executionContext = initExecutionContext(balanceSoFar);
        stepExecution.setExecutionContext(executionContext);
        // This part is mocking and uses MetaDataInstanceFactory

        FillBalanceProcessor processor = new FillBalanceProcessor();
        processor.setStepExecution(stepExecution);

        BigDecimal transactionAmount = BigDecimal.valueOf(RANDOM.nextDouble());
        processor.process(new BankTransaction(1, 1, 1, 1, 1,
                          transactionAmount, UUID.randomUUID().toString()));

        Assertions.assertEquals(stepExecution.getExecutionContext()
                                        .getDouble(FillBalanceProcessor.BALANCE_SO_FAR),
                                transactionAmount.add(BigDecimal.valueOf(balanceSoFar))
                                        .setScale(2, RoundingMode.HALF_UP)
                                        .doubleValue(),
                          // Implementation only cares about scale of 2
                          0.01);
    }

    @Test
    public void testProcessorWithMockito() throws Exception {
        double balanceSoFar = RANDOM.nextDouble();

        // This part is mocking and uses Mockito
        StepExecution stepExecution = Mockito.mock(StepExecution.class);
        ExecutionContext executionContext = initExecutionContext(balanceSoFar);
        Mockito.when(stepExecution.getExecutionContext()).thenReturn(executionContext);
        // This part is mocking and uses Mockito

        FillBalanceProcessor processor = new FillBalanceProcessor();
        processor.setStepExecution(stepExecution);

        BigDecimal transactionAmount = BigDecimal.valueOf(RANDOM.nextDouble());
        processor.process(new BankTransaction(1, 1, 1, 1, 1,
                transactionAmount, UUID.randomUUID().toString()));

        Assertions.assertEquals(stepExecution.getExecutionContext()
                        .getDouble(FillBalanceProcessor.BALANCE_SO_FAR),
                transactionAmount.add(BigDecimal.valueOf(balanceSoFar))
                        .setScale(2, RoundingMode.HALF_UP)
                        .doubleValue(),
                // Implementation only cares about scale of 2
                0.01);
    }

    // Initializes the context with balance-so-far value
    private ExecutionContext initExecutionContext(double value) {
        ExecutionContext context = new ExecutionContext();
        context.putDouble(FillBalanceProcessor.BALANCE_SO_FAR, value);
        return context;
    }
}
