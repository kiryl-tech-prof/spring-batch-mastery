package org.example;

import org.springframework.batch.core.StepExecution;
import org.springframework.batch.item.ItemProcessor;

import java.math.BigDecimal;
import java.math.RoundingMode;


/**
 * Processor taking the item and calculating the update which consists
 * of the balance after the transaction is complete, and identifier of the transaction
 */
public class FillBalanceProcessor implements ItemProcessor<BankTransaction, BalanceUpdate> {

    public static final String BALANCE_SO_FAR = "balanceSoFar";

    // Step execution variable needs to be set in order for processor to be executed
    private StepExecution stepExecution;

    @Override
    public BalanceUpdate process(BankTransaction item) throws Exception {
        if (stepExecution == null) {
            throw new RuntimeException("Can not process item without accessing the step execution");
        }

        BigDecimal newBalance = BigDecimal.valueOf(getLatestTransactionBalance())
                .setScale(2, RoundingMode.HALF_UP)
                .add(item.getAmount());
        BalanceUpdate balanceUpdate = new BalanceUpdate(item.getId(), newBalance);
        stepExecution.getExecutionContext().putDouble(BALANCE_SO_FAR, newBalance.doubleValue());
        return balanceUpdate;
    }

    public double getLatestTransactionBalance() {
        if (stepExecution == null) {
            throw new RuntimeException("Can not get the latest balance without accessing the step execution");
        }
        // If no balance is present, start from 0
        return stepExecution.getExecutionContext().getDouble(BALANCE_SO_FAR, 0d);
    }

    // Step execution need to be set when step execution is relevant, and cleared when no longer relevant
    public void setStepExecution(StepExecution stepExecution) {
        this.stepExecution = stepExecution;
    }
}
