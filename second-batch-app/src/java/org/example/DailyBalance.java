package org.example;

import com.google.common.collect.ImmutableMap;
import org.springframework.batch.item.database.Order;
import org.springframework.batch.item.database.PagingQueryProvider;
import org.springframework.batch.item.database.support.PostgresPagingQueryProvider;
import org.springframework.jdbc.core.RowMapper;

import java.math.BigDecimal;


// Entity representing balance aggregated by day & month
public class DailyBalance {

    private final int day;
    private final int month;
    private final BigDecimal balance;

    public DailyBalance(int day, int month, BigDecimal balance) {
        this.day = day;
        this.month = month;
        this.balance = balance;
    }

    // Row mapper to transform query results into Java object
    public static final RowMapper<DailyBalance> ROW_MAPPER = (rs, rowNum) -> new DailyBalance(
            rs.getInt("day"),
            rs.getInt("month"),
            rs.getBigDecimal("balance")
    );

    // Query provider to obtain daily balance aggregation from 'bank_transaction_yearly' table
    public static PagingQueryProvider getQueryProvider() {
        PostgresPagingQueryProvider queryProvider = new PostgresPagingQueryProvider();
        queryProvider.setSelectClause("sum(amount) as balance, day, month");
        queryProvider.setFromClause("bank_transaction_yearly");
        queryProvider.setGroupClause("day, month");
        queryProvider.setSortKeys(ImmutableMap.<String, Order>builder()
                .put("month", Order.ASCENDING)
                .put("day", Order.ASCENDING)
                .build());
        return queryProvider;
    }

    public int getDay() {
        return day;
    }

    public int getMonth() {
        return month;
    }

    public BigDecimal getBalance() {
        return balance;
    }
}
