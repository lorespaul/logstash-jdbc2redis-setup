package com.lorenzodaneo.messagebroker;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.PendingMessages;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.SessionCallback;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Slf4j
public class RedisAssignmentsCoordinator {

    private final Callable<List<String>> getStreams;
    private final RedisTemplate<String, String> redisTemplate;
    private final RedissonClient redissonClient;
    private final ObjectMapper mapper;
    private final String applicationName;
    private final List<Integer> partitions;
    private Timer timer = null;
    private RLock applicationCoordinatorLock = null;

    public RedisAssignmentsCoordinator(Callable<List<String>> getStreams,
                                       RedisTemplate<String, String> redisTemplate,
                                       RedissonClient redissonClient,
                                       ObjectMapper mapper,
                                       String applicationName,
                                       int partitionsCount){
        this.getStreams = getStreams;
        this.redisTemplate = redisTemplate;
        this.redissonClient = redissonClient;
        this.mapper = mapper;
        this.applicationName = applicationName;
        this.partitions = IntStream.range(0, partitionsCount).boxed().collect(Collectors.toList());
    }

    public void start(){
        applicationCoordinatorLock = getApplicationCoordinatorLock();
        if(applicationCoordinatorLock != null){
            timer = new Timer();
            timer.schedule(new TimerTask() {
                @Override
                public void run() {
                    findAndFixBrokenAssignments();
                }
            }, 0, 60000);
        }
    }

    @SneakyThrows
    protected void findAndFixBrokenAssignments() {
        List<String> streams = getStreams.call();
        streams.forEach(baseChannel -> {
            RLock lock = getChannelAssignmentsLock(baseChannel);
            if(lock == null)
                return;
            try {
                String channelAssignmentKey = RedisUtils.getChannelAssignmentsKey(applicationName, baseChannel);
                String strAssignments = redisTemplate.opsForValue().get(channelAssignmentKey);
                if(strAssignments != null && !strAssignments.isEmpty()){
                    Assignments assignments = mapper.readValue(strAssignments, Assignments.class);
                    findInactiveConsumers(baseChannel, assignments);
                }
            } catch (Exception e){
                log.warn("Exception during execution of coordinator for channel {}", baseChannel, e);
            } finally {
                lock.unlock();
            }
        });
    }

    private List<ConsumerGroup> findInactiveConsumers(String baseChannel, Assignments assignments){
        List<PendingMessages> pendingMessagesList = findPendingMessages(baseChannel, assignments);
        // TODO: xclaim pending messages and delete consumers
        return new ArrayList<>();
    }

    private List<PendingMessages> findPendingMessages(String baseChannel, Assignments assignments){
        return redisTemplate.executePipelined(new SessionCallback<Object>() {
            @Override
            public <K, V> Object execute(RedisOperations<K, V> redisOperations) throws DataAccessException {
                for(int i : partitions) {
                    String channel = RedisUtils.getPartitionedChannel(baseChannel, i);
                    List<String> strConsumerGroups = assignments.getConsumerGroupsByPartitionedChannel(i);
                    List<ConsumerGroup> consumerGroups = strConsumerGroups
                            .stream()
                            .map(x -> new ConsumerGroup(
                                    RedisUtils.getGroupFromConsumerGroupKey(x),
                                    RedisUtils.getConsumerFromConsumerGroupKey(x),
                                    baseChannel
                            ))
                            .collect(Collectors.toList());
                    consumerGroups.forEach(cg -> redisTemplate
                            .opsForStream()
                            .pending(
                                    channel,
                                    Consumer.from(cg.getGroup(), cg.getConsumer())
                            ));
                }
                return null;
            }
        }).stream().map(x -> (PendingMessages)x).filter(x -> !x.isEmpty()).collect(Collectors.toList());
    }

    private RLock getApplicationCoordinatorLock() {
        String lockKey = RedisUtils.getApplicationCoordinatorLockKey(applicationName);
        return getLock(lockKey, 1);
    }

    private RLock getChannelAssignmentsLock(String baseChannel) {
        String lockKey = RedisUtils.getChannelAssignmentsLockKey(applicationName, baseChannel);
        return getLock(lockKey, 30);
    }

    private RLock getLock(String lockKey, int retryTimeSeconds) {
        RLock lock = redissonClient.getLock(lockKey);
        try{
            if(lock.tryLock(retryTimeSeconds, TimeUnit.SECONDS))
                return lock;
        } catch (Exception e){
            log.error("Error during lock acquire in coordinator for key {}", lockKey, e);
        }
        return null;
    }

    public void close(){
        if(timer != null)
            timer.cancel();
        if(applicationCoordinatorLock != null)
           applicationCoordinatorLock.unlock();
    }

    @Getter
    @AllArgsConstructor
    private static class ConsumerGroup {
        private final String group;
        private final String consumer;
        private final String baseChannel;
        private final List<Integer> partitions = new ArrayList<>();
    }

}
