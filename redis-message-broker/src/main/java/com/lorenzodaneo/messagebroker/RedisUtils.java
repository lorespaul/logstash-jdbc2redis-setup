package com.lorenzodaneo.messagebroker;

public class RedisUtils {

    public static String getQueueStreamKey(String channel, int partition){
        return RedisConstants.QUEUE + getPartitionedChannel(channel, partition);
    }

    public static String getTopicStreamKey(String channel, int partition){
        return RedisConstants.TOPIC + getPartitionedChannel(channel, partition);
    }

    public static String getConsumerGroupKey(String group, String consumer){
        return String.format("%s-%s", group, consumer);
    }

    public static String getPartitionedChannel(String baseChannel, int partition){
        return String.format("%s-%s", baseChannel, partition);
    }

    public static Integer getPartitionFromPartitionedChannel(String channel){
        String[] channelSplit = channel.split("-");
        return Integer.parseInt(channelSplit[channelSplit.length - 1]);
    }

    public static String getChannelAssignmentsKey(String channel){
        return String.format("assignments:%s", channel);
    }

    public static String getChannelAssignmentsLockKey(String channel){
        return String.format("lock:assignments:%s", channel);
    }

}
