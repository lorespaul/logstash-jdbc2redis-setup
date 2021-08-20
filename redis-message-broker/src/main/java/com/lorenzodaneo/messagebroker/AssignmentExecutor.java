package com.lorenzodaneo.messagebroker;

public interface AssignmentExecutor {
    void executeAssignment(String channel, String group, String consumer) throws Exception;
}
