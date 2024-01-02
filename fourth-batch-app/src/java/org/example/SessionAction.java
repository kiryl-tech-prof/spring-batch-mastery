package org.example;


/**
 * Domain entity for session action table, which schema
 * is defined at org.example.SourceDatabaseUtils.createSessionActionTable
 */
public class SessionAction {

    // Table name for session actions
    public static final String SESSION_ACTION_TABLE_NAME = "session_action";

    private final long id;
    private final long userId;
    private final String actionType;
    private final double amount;

    public SessionAction(long id, long userId, String actionType, double amount) {
        this.id = id;
        this.userId = userId;
        this.actionType = actionType;
        this.amount = amount;
    }

    public long getId() {
        return id;
    }

    public long getUserId() {
        return userId;
    }

    public String getActionType() {
        return actionType;
    }

    public double getAmount() {
        return amount;
    }
}
