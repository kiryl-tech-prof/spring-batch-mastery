package org.example;

import org.springframework.batch.item.database.ItemPreparedStatementSetter;
import org.springframework.batch.item.database.Order;
import org.springframework.batch.item.database.PagingQueryProvider;
import org.springframework.batch.item.database.support.PostgresPagingQueryProvider;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

import java.util.Collections;


// Utility class to encapsulate the logic of manipulating schema and records of the source database
public class SourceDatabaseUtils {

    // Constants for action types
    public static final String PLUS_TYPE = "plus";
    public static final String MULTI_TYPE = "multi";

    public static void dropTableIfExists(JdbcTemplate jdbcTemplate, String tableName) {
        jdbcTemplate.update("drop table if exists " + tableName);
    }

    // Creates a schema for session action table
    public static void createSessionActionTable(JdbcTemplate jdbcTemplate, String tableName) {
        jdbcTemplate.update("create table " + tableName + " (" +
                "id serial primary key," +
                "user_id int not null," +
                // Either 'plus' or 'multi'
                "action_type varchar(36) not null," +
                "amount numeric(10,2) not null" +
                ")");
    }

    // Creates a schema for user score table
    public static void createUserScoreTable(JdbcTemplate jdbcTemplate, String tableName) {
        jdbcTemplate.update("create table " + tableName + " (" +
                "user_id int not null unique," +
                "score numeric(10,2) not null" +
                ")");
    }

    // Upsert query for user (lazy insert record if not present yet)
    public static String constructUpdateUserScoreQuery(String tableName) {
        return "insert into " + tableName + " (user_id, score) values (?, ?) " +
                "on conflict (user_id) do " +
                "update set score = " + tableName + ".score * ? + ?";
    }

    // Parameter setter for org.example.SourceDatabaseUtils.constructUpdateUserScoreQuery
    public static ItemPreparedStatementSetter<UserScoreUpdate> UPDATE_USER_SCORE_PARAMETER_SETTER = (item, ps) -> {
        ps.setLong(1, item.getUserId());
        ps.setDouble(2, item.getAdd());
        ps.setDouble(3, item.getMultiply());
        ps.setDouble(4, item.getAdd());
    };

    // Insert session action record into the specified table
    public static void insertSessionAction(JdbcTemplate jdbcTemplate, SessionAction sessionAction, String tableName) {
        jdbcTemplate
                .update("insert into " + tableName + " (id, user_id, action_type, amount) values (?, ?, ?, ?)",
                        sessionAction.getId(), sessionAction.getUserId(), sessionAction.getActionType(), sessionAction.getAmount());
    }

    // Query provider to select all records from session actions table with the specified name
    public static PostgresPagingQueryProvider selectAllSessionActionsProvider(String tableName) {
        PostgresPagingQueryProvider queryProvider = new PostgresPagingQueryProvider();
        queryProvider.setSelectClause("id, user_id, action_type, amount");
        queryProvider.setFromClause(tableName);
        queryProvider.setSortKeys(Collections.singletonMap("id", Order.ASCENDING));
        return queryProvider;
    }

    // Query provider to select partition-specific records: (userId % pCount) == pIndex from session actions table
    public static PagingQueryProvider selectPartitionOfSessionActionsProvider(String tableName,
                                                                              int partitionCount, int partitionIndex) {
        PostgresPagingQueryProvider queryProvider = selectAllSessionActionsProvider(tableName);
        queryProvider.setWhereClause("user_id % " + partitionCount + " = " + partitionIndex);
        return queryProvider;
    }

    // Row mapper for the session action record
    public static RowMapper<SessionAction> getSessionActionMapper() {
        return (rs, rowNum) ->
                new SessionAction(rs.getLong("id"), rs.getLong("user_id"),
                                  rs.getString("action_type"), rs.getDouble("amount"));
    }
}
