package com.lorenzodaneo.messagebroker;

public class RedisConstants {
    public static final int BLOCK_TIMEOUT_MILLIS = 3000;

    public static final String KEY_MESSAGE = "message";
    public static final String QUEUE = "queue:";
    public static final String TOPIC = "topic:";

    public static final String CONSUMER_GROUP_DIVIDER = "$$";
}
