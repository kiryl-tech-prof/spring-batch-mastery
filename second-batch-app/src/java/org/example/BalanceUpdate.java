package org.example;

import java.math.BigDecimal;


// Domain entity representing an update to bank transaction balance
public class BalanceUpdate {

    private final long id;
    private final BigDecimal balance;

    public BalanceUpdate(long id, BigDecimal balance) {
        this.id = id;
        this.balance = balance;
    }

    public long getId() {
        return id;
    }

    public BigDecimal getBalance() {
        return balance;
    }
}
