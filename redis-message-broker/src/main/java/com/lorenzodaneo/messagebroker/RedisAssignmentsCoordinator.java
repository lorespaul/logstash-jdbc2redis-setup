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
import org.springframework.data.redis.connection.stream.PendingMessage;
import org.springframework.data.redis.connection.stream.PendingMessages;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.SessionCallback;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
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
    private final ExecutorService lockExecutorService;
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
        this.lockExecutorService = Executors.newSingleThreadExecutor();
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
            }, 60000, 60000);
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
                    List<ChannelGroupPendingMessages> channelGroupPendingMessages = findPendingMessages(baseChannel, assignments);
                    List<ConsumerPendingMessages> consumerPendingMessages = findInactiveConsumers(channelGroupPendingMessages);

                }
            } catch (Exception e){
                log.warn("Exception during execution of coordinator for channel {}", baseChannel, e);
            } finally {
                lock.unlock();
            }
        });
    }

    private List<ChannelGroupPendingMessages> findPendingMessages(String baseChannel, Assignments assignments){
        AtomicInteger index = new AtomicInteger(0);
        return redisTemplate.executePipelined(new SessionCallback<Object>() {
            @Override
            public <K, V> Object execute(RedisOperations<K, V> redisOperations) throws DataAccessException {
                for(int i : partitions) {
                    String channel = RedisUtils.getPartitionedChannel(baseChannel, i);
                    List<String> strConsumerGroups = assignments.getConsumerGroupsByPartitionedChannel(i);
                    List<ConsumerPendingMessages> consumerPendingMessages = strConsumerGroups
                            .stream()
                            .map(x -> new ConsumerPendingMessages(
                                    RedisUtils.getGroupFromConsumerGroupKey(x),
                                    RedisUtils.getConsumerFromConsumerGroupKey(x),
                                    channel,
                                    null
                            ))
                            .collect(Collectors.toList());
                    consumerPendingMessages.forEach(cg -> redisOperations
                            .opsForStream()
                            .pending(
                                    (K)cg.getChannel(),
                                    Consumer.from(cg.getGroup(), cg.getConsumer())
                            ));
                }
                return null;
            }
        }).stream()
                .map(x -> new ChannelGroupPendingMessages(
                        ((PendingMessages)x).getGroupName(),
                        RedisUtils.getPartitionedChannel(baseChannel, index.getAndIncrement()),
                        (PendingMessages)x
                ))
                .filter(x -> !x.getPendingMessages().isEmpty())
                .collect(Collectors.toList());
    }

    private List<ConsumerPendingMessages> findInactiveConsumers(List<ChannelGroupPendingMessages> channelGroupPendingMessages){
        List<ConsumerPendingMessages> consumerPendingMessages = new ArrayList<>();
        for(ChannelGroupPendingMessages cg : channelGroupPendingMessages){
            List<String> consumersNames = cg.getPendingMessages()
                    .stream()
                    .map(PendingMessage::getConsumerName)
                    .distinct()
                    .collect(Collectors.toList());
            List<ConsumerPendingMessages> pendingMessages = redisTemplate.opsForStream()
                    .consumers(cg.getChannel(), cg.getGroup())
                    .stream().filter(x -> consumersNames.contains(x.consumerName()) && x.idleTimeMs() > 60000)
                    .map(infoConsumer -> new ConsumerPendingMessages(
                            infoConsumer.groupName(),
                            infoConsumer.consumerName(),
                            cg.getChannel(),
                            cg.getPendingMessages().stream()
                                    .filter(x -> Objects.equals(infoConsumer.consumerName(), x.getConsumerName()))
                                    .collect(Collectors.toList())
                    ))
                    .collect(Collectors.toList());
            consumerPendingMessages.addAll(pendingMessages);
        }
        return consumerPendingMessages;
    }




    @SneakyThrows
    private RLock getApplicationCoordinatorLock() {
        return lockExecutorService.submit(() -> {
            String lockKey = RedisUtils.getApplicationCoordinatorLockKey(applicationName);
            return getLock(lockKey, 1);
        }).get();
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

    public void close() throws Exception {
        if(timer != null)
            timer.cancel();
        if(applicationCoordinatorLock != null)
           lockExecutorService.submit(() -> applicationCoordinatorLock.unlock()).get();
    }

    @Getter
    @AllArgsConstructor
    private static class ChannelGroupPendingMessages {
        private final String group;
        private final String channel;
        private final PendingMessages pendingMessages;
    }

    @Getter
    @AllArgsConstructor
    private static class ConsumerPendingMessages {
        private final String group;
        private final String consumer;
        private final String channel;
        private final List<PendingMessage> pendingMessages;
    }

}
