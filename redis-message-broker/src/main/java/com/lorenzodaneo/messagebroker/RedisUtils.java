package com.lorenzodaneo.messagebroker;

import lombok.SneakyThrows;

public class RedisUtils {

    public static String getQueueStreamKey(String channel, int partition){
        return RedisConstants.QUEUE + getStreamKey(channel, partition);
    }

    public static String getTopicStreamKey(String channel, int partition){
        return RedisConstants.TOPIC + getStreamKey(channel, partition);
    }

    private static String getStreamKey(String channel, int partition){
        return channel + "-" + partition;
    }

    public static String getConsumerGroupKey(String group, String consumer){
        return String.format("%s-%s", group, consumer);
    }

    public static String getPartitionedChannel(String baseChannel, int partition){
        return String.format("%s-%s", baseChannel, partition);
    }

    public static String getChannelAssignmentsKey(String channel){
        return String.format("assignments:%s", channel);
    }

    public static String getChannelAssignmentsLockKey(String channel){
        return String.format("lock:assignments:%s", channel);
    }

    @SneakyThrows
    public static void sleepASecond() {
        Thread.sleep(1000);
    }

}
