package com.lorenzodaneo.messagebroker;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Getter
@AllArgsConstructor
public class AssignmentExecutorWrapper {
    private final String group;
    private final String consumer;
    private final CancelChannelSubscription cancelSubscription;
    private final AssignmentExecutor assignmentExecutor;
    private final List<String> runningChannels = Collections.synchronizedList(new ArrayList<>());
}
