package com.lorenzodaneo.messagebroker;

import java.util.regex.Pattern;

public class RedisUtils {

    public static String getQueueStreamKey(String channel, int partition){
        return RedisConstants.QUEUE + getPartitionedChannel(channel, partition);
    }

    public static String getTopicStreamKey(String channel, int partition){
        return RedisConstants.TOPIC + getPartitionedChannel(channel, partition);
    }

    public static String getConsumerGroupKey(String group, String consumer){
        return String.format("%s%s%s", group, RedisConstants.CONSUMER_GROUP_DIVIDER, consumer);
    }

    public static String getGroupFromConsumerGroupKey(String consumerGroupKey){
        return consumerGroupKey.split(Pattern.quote(RedisConstants.CONSUMER_GROUP_DIVIDER))[0];
    }

    public static String getConsumerFromConsumerGroupKey(String consumerGroupKey){
        return consumerGroupKey.split(Pattern.quote(RedisConstants.CONSUMER_GROUP_DIVIDER))[1];
    }

    public static String getPartitionedChannel(String baseChannel, int partition){
        return String.format("%s-%s", baseChannel, partition);
    }

    public static Integer getPartitionFromPartitionedChannel(String channel){
        String[] channelSplit = channel.split("-");
        return Integer.parseInt(channelSplit[channelSplit.length - 1]);
    }

    public static String getChannelAssignmentsKey(String applicationName, String channel){
        return String.format("assignments:%s:%s", applicationName, channel);
    }

    public static String getChannelAssignmentsLockKey(String applicationName, String channel){
        return String.format("lock:assignments:%s:%s", applicationName, channel);
    }

    public static String getApplicationCoordinatorLockKey(String applicationName){
        return String.format("lock:coordinator:%s", applicationName);
    }

}
