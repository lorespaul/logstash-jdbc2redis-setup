package com.lorenzodaneo.messagebroker;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.RedisTemplate;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Slf4j
public class RedisMessageBrokerImpl implements DisposableBean {

    private final RedisTemplate<String, String> redisTemplate;
    private final ObjectMapper mapper;
    private final RedisConsumerAssignmentsManager consumerAssignmentsManager;
    private final RedisAssignmentsCoordinator assignmentsCoordinator;
    private final int partitionsCount;
    private final List<RedisMultiSubscription> subscriptions = Collections.synchronizedList(new ArrayList<>());

    public RedisMessageBrokerImpl(RedisTemplate<String, String> redisTemplate,
                                  RedissonClient redissonClient,
                                  ObjectMapper mapper,
                                  int poolSize,
                                  int partitionsCount){
        this.redisTemplate = redisTemplate;
        this.mapper = mapper;
        this.consumerAssignmentsManager = new RedisConsumerAssignmentsManager(redisTemplate, redissonClient, mapper, poolSize, partitionsCount);
        this.assignmentsCoordinator = new RedisAssignmentsCoordinator(this.consumerAssignmentsManager::getStreamConsumers, redissonClient);
        this.partitionsCount = partitionsCount;
    }

    @SneakyThrows
    public <T extends Message> void sendMessageToQueue(String channel, T message) {
        String myChannel = RedisUtils.getQueueStreamKey(channel, Math.abs((int)(message.getAggregateId() % partitionsCount)));
        sendMessage(myChannel, message);
    }

    @SneakyThrows
    public <T extends Message> void sendMessageToTopic(String channel, T message) {
        String myChannel = RedisUtils.getTopicStreamKey(channel, Math.abs((int)(message.getAggregateId() % partitionsCount)));
        sendMessage(myChannel, message);
    }

    private <T extends Message> void sendMessage(String channel, T message) throws JsonProcessingException {
        MessageWrapper<T> myMessage = new MessageWrapper<>(message.getAggregateId(), message);
        String serializedMessage = mapper.writeValueAsString(myMessage);
        StringRecord record = StreamRecords
                .string(Collections.singletonMap(RedisConstants.KEY_MESSAGE, serializedMessage))
                .withStreamKey(channel);
        log.info("Sending message on channel {} : {}", channel, serializedMessage);
        redisTemplate.opsForStream().add(record);
    }

    public <T> void subscribeToQueue(final String channel, final Class<T> clazz, final Action<T> handler){
        final String group = "default";
        subscribe(RedisConstants.QUEUE + channel, group, clazz, handler);
    }

    public <T> void subscribeToTopic(final String channel, final String group, final Class<T> clazz, final Action<T> handler){
        subscribe(RedisConstants.TOPIC + channel, group, clazz, handler);
    }

    private <T> void subscribe(final String baseChannel, final String group, final Class<T> clazz, final Action<T> handler){
        final RedisMultiSubscription subscription = new RedisMultiSubscription(redisTemplate, mapper);
        consumerAssignmentsManager.addStreamConsumer(
                baseChannel,
                group,
                (c) -> {
                    subscription.unsubscribe(c);
                    if(subscription.getRunningCount() == 0){
                        subscriptions.remove(subscription);
                        subscription.close();
                    }
                },
                (ch, gr, co) -> subscription.subscribe(ch, gr, co, clazz, handler)
        );
        subscriptions.add(subscription);
    }


    @Override
    public void destroy() {
        consumerAssignmentsManager.close();
        assignmentsCoordinator.close();
        synchronized (subscriptions){
            for(RedisMultiSubscription subscription : subscriptions)
                subscription.close();
        }
        subscriptions.clear();
        log.info("End clearing subscriptions");
    }
}
