package org.example;

import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;


// Utility class with handy functions to manage the schema of database source
public class SourceManagementUtils {

    /**
     * Alter the schema of the table 'bank_transaction_yearly' (if not yet altered) by adding new 'balance' column
     *
     * @param dataSource database connectivity data source
     */
    public static void addBalanceColumn(DataSource dataSource) {
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);

        /*
        This method should be used with Postgresql older than 9.6

        Integer columnCount = jdbcTemplate
                .queryForObject("select count(*) from information_schema.columns " +
                                "where table_name = ? and column_name= ?",
                                Integer.class, "bank_transaction_yearly", "balance");

        if (columnCount != null && columnCount == 0) {
            // Only if such column does not exist, we create it
        }
         */

        jdbcTemplate.update("alter table bank_transaction_yearly add column if not exists balance numeric(10,2)");
    }

    /**
     * Alter the schema of the table 'bank_transaction_yearly' (if not yet altered) by adding new 'adjusted' column
     *
     * @param dataSource database connectivity data source
     */
    public static void addAdjustedColumn(DataSource dataSource) {
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);

        /*
        This method should be used with Postgresql older than 9.6

        Integer columnCount = jdbcTemplate
                .queryForObject("select count(*) from information_schema.columns " +
                                "where table_name = ? and column_name= ?",
                                Integer.class, "bank_transaction_yearly", "adjusted");

        if (columnCount != null && columnCount == 0) {
            // Only if such column does not exist, we create it
        }
         */

        jdbcTemplate.update("alter table bank_transaction_yearly add column if not exists adjusted boolean default false");
    }

    // Initializes the database schema: first drops the table (if exists), then creates it
    // No data is inserted as a result of this method
    public static void initializeEmptyDatabase(JdbcTemplate jdbcTemplate) {
        // Drop table if exist
        jdbcTemplate.update("drop table if exists bank_transaction_yearly");
        // Create the table
        jdbcTemplate.update("create table bank_transaction_yearly (" +
                "id serial primary key," +
                "month int not null," +
                "day int not null," +
                "hour int not null," +
                "minute int not null," +
                "amount numeric(10,2) not null," +
                "merchant varchar(36) not null" +
                ")");
    }

    // Inserts new transaction in the database. Please note that the 'id' property is ignored,
    // method relies on the database to autofill it (serial / auto-increment)
    public static void insertBankTransaction(BankTransaction transaction, JdbcTemplate jdbcTemplate) {
        jdbcTemplate.update("insert into bank_transaction_yearly (month, day, hour, minute, amount, merchant) " +
                        "values (?, ?, ?, ?, ?, ?)",
                transaction.getMonth(), transaction.getDay(), transaction.getHour(),
                transaction.getMinute(), transaction.getAmount(), transaction.getMerchant());
    }
}
