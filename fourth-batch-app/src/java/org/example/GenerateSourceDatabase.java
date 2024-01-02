package org.example;

import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;
import java.util.Random;


public class GenerateSourceDatabase {

    private static final int USER_COUNT = 100;
    private static final int RECORD_COUNT = 1000;
    private static final Random RANDOM = new Random();

    // Drop the table, create the table and fill it out with the data
    public static void main(String[] args) {
        // We are re-using Spring Context to get connection properties same way as in Spring Batch
        ApplicationContext context = new AnnotationConfigApplicationContext(SourceConfiguration.class);
        JdbcTemplate jdbcTemplate = new JdbcTemplate(context.getBean(DataSource.class));

        SourceDatabaseUtils.dropTableIfExists(jdbcTemplate, SessionAction.SESSION_ACTION_TABLE_NAME);
        SourceDatabaseUtils.createSessionActionTable(jdbcTemplate, SessionAction.SESSION_ACTION_TABLE_NAME);

        for (int i = 0; i < RECORD_COUNT; i++) {
            SourceDatabaseUtils.insertSessionAction(jdbcTemplate, generateRecord(i + 1), SessionAction.SESSION_ACTION_TABLE_NAME);
        }

        // Print to console the success message
        System.out.println("Input source table with " + RECORD_COUNT + " records is successfully initialized");
    }

    // Generate random session action record
    private static SessionAction generateRecord(long id) {
        long userId = 1 + RANDOM.nextInt(USER_COUNT);
        return RANDOM.nextBoolean()
                ? new SessionAction(id, userId, SourceDatabaseUtils.PLUS_TYPE, 1 + RANDOM.nextInt(3))
                : new SessionAction(id, userId, SourceDatabaseUtils.MULTI_TYPE, ((double) (1 + RANDOM.nextInt(5))) / 10 + 1d);
    }



}
