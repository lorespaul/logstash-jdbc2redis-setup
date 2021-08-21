package com.lorenzodaneo.messagebroker;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.data.redis.core.RedisTemplate;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.UUID.randomUUID;

@Slf4j
public class RedisConsumerAssignmentsManager {

    private final RedisTemplate<String, String> redisTemplate;
    private final RedissonClient redissonClient;
    private final ObjectMapper mapper;
    private final ExecutorService fixedExecutorService;
    private final int partitionsCount;
    private final Map<String, List<AssignmentExecutorWrapper>> streamConsumers = Collections.synchronizedMap(new HashMap<>());
    private final Timer timer = new Timer();

    public RedisConsumerAssignmentsManager(RedisTemplate<String, String> redisTemplate,
                                           RedissonClient redissonClient,
                                           ObjectMapper mapper,
                                           int poolSize,
                                           int partitionsCount){
        this.redisTemplate = redisTemplate;
        this.redissonClient = redissonClient;
        this.mapper = mapper;
        this.fixedExecutorService = Executors.newFixedThreadPool(poolSize);
        this.partitionsCount = partitionsCount;
        this.start();
    }

    public Map<String, List<AssignmentExecutorWrapper>> getStreamConsumers(){
        synchronized (streamConsumers){
            return streamConsumers
                    .entrySet()
                    .stream()
                    .collect(Collectors.toMap(
                            Map.Entry::getKey,
                            e -> new ArrayList<>(e.getValue())
                    ));
        }
    }

    public void addStreamConsumer(String baseChannel, String group, CancelChannelSubscription cancelSubscription, AssignmentExecutor executor){
        synchronized (streamConsumers){
            if(!streamConsumers.containsKey(baseChannel))
                streamConsumers.put(baseChannel, new ArrayList<>());
            AssignmentExecutorWrapper executorWrapper = new AssignmentExecutorWrapper(group, randomUUID().toString(), cancelSubscription, executor);
            streamConsumers.get(baseChannel).add(executorWrapper);
            initAssignmentsAndRebalance(baseChannel, executorWrapper);
        }
    }

    private void start(){
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                pullAssignmentsAndRebalance();
            }
        }, 0, 10000);
    }

    protected void rebalanceExecutors(String baseChannel, Assignments assignments, AssignmentExecutorWrapper executorWrapper){
        String consumerGroup = RedisUtils.getConsumerGroupKey(executorWrapper.getGroup(), executorWrapper.getConsumer());
        List<Integer> currentPartitions = executorWrapper
                .getRunningChannels()
                .stream()
                .map(RedisUtils::getPartitionFromPartitionedChannel)
                .collect(Collectors.toList());
        List<Integer> expectedPartitions = Optional
                .ofNullable(assignments.getAssignmentsByConsumer().get(consumerGroup))
                .orElseGet(ArrayList::new);

        List<Integer> stoppingPartitions = currentPartitions
                .stream()
                .filter(currentPartition -> !expectedPartitions.contains(currentPartition))
                .collect(Collectors.toList());

        List<Integer> startingPartitions = expectedPartitions
                .stream()
                .filter(expectedPartition -> !currentPartitions.contains(expectedPartition))
                .collect(Collectors.toList());

        stoppingPartitions.forEach(n -> {
            String channel = RedisUtils.getPartitionedChannel(baseChannel, n);
            executorWrapper.getRunningChannels().remove(channel);
            executorWrapper.getCancelSubscription().cancel(channel);
        });

        startingPartitions.forEach(n -> {
            String channel = RedisUtils.getPartitionedChannel(baseChannel, n);
            executorWrapper.getRunningChannels().add(channel);
            fixedExecutorService.submit(() -> {
                try {
                    executorWrapper.getAssignmentExecutor().executeAssignment(channel, executorWrapper.getGroup(), executorWrapper.getConsumer());
                } catch (Exception e) {
                    executorWrapper.getRunningChannels().remove(channel);
                }
            });
        });
    }

    // INIT ASSIGNMENTS
    protected void initAssignmentsAndRebalance(String baseChannel, AssignmentExecutorWrapper executorWrapper){
        Assignments assignments = initAssignmentsByChannel(baseChannel, executorWrapper.getGroup(), executorWrapper.getConsumer());
        rebalanceExecutors(baseChannel, assignments, executorWrapper);
    }

    protected Assignments initAssignmentsByChannel(final String baseChannel, final String group, final String consumer){
        String lockKey = RedisUtils.getChannelAssignmentsLockKey(baseChannel);
        RLock lock = null;
        try{
            lock = getLock(lockKey);

            String channelAssignmentKey = RedisUtils.getChannelAssignmentsKey(baseChannel);
            String strAssignments = redisTemplate.opsForValue().get(channelAssignmentKey);

            Assignments assignments = strAssignments != null && !strAssignments.isEmpty() ?
                    mapper.readValue(strAssignments, Assignments.class) :
                    new Assignments();
            createConsumer(group, consumer, assignments);

            redisTemplate.opsForValue().set(channelAssignmentKey, mapper.writeValueAsString(assignments));
            return assignments;
        } catch (Throwable e){
            throw new RuntimeException(String.format("Error init assignments on group %s and consumer %s for channel %s", group, consumer, baseChannel), e);
        } finally {
            if(lock != null)
                lock.unlock();
        }
    }

    // PULL ASSIGNMENTS
    protected void pullAssignmentsAndRebalance(){
        synchronized (streamConsumers){
            streamConsumers.forEach((key, value) -> value.forEach(executorWrapper -> {
                Assignments assignments = pullAssignmentsByChannel(key);
                if (assignments != null)
                    rebalanceExecutors(key, assignments, executorWrapper);
            }));
        }
    }

    protected Assignments pullAssignmentsByChannel(final String baseChannel){
        String channelAssignmentsKey = RedisUtils.getChannelAssignmentsKey(baseChannel);
        String strAssignments = redisTemplate.opsForValue().get(channelAssignmentsKey);
        if(strAssignments != null && !strAssignments.isEmpty()){
            try {
                return mapper.readValue(strAssignments, Assignments.class);
            } catch (JsonProcessingException ignored){}
        }
        return null;
    }

    // REMOVE ASSIGNMENTS
    protected void removeAssignmentsAndRebalance(){
        synchronized (streamConsumers){
            streamConsumers.forEach((key, value) -> value.forEach(executorWrapper -> {
                Assignments assignments = removeAssignmentsByChannel(key, executorWrapper.getGroup(), executorWrapper.getConsumer());
                if(assignments != null)
                    rebalanceExecutors(key, assignments, executorWrapper);
                log.info("Stop group {} and consumer {} for channel {}", executorWrapper.getGroup(), executorWrapper.getConsumer(), key);
            }));
        }
    }

    protected Assignments removeAssignmentsByChannel(final String baseChannel, final String group, final String consumer){
        String lockKey = RedisUtils.getChannelAssignmentsLockKey(baseChannel);
        RLock lock = null;
        try{
            lock = getLock(lockKey);

            String channelAssignmentsKey = RedisUtils.getChannelAssignmentsKey(baseChannel);
            String strAssignments = redisTemplate.opsForValue().get(channelAssignmentsKey);

            Assignments assignments = mapper.readValue(strAssignments, Assignments.class);
            destroyConsumer(group, consumer, assignments);

            redisTemplate.opsForValue().set(channelAssignmentsKey, mapper.writeValueAsString(assignments));
            return assignments;
        } catch (Throwable e){
            log.error("Error removing assignments on group {} and consumer {} for channel {}", group, consumer, baseChannel, e);
        } finally {
            if(lock != null)
                lock.unlock();
        }
        return null;
    }

    // MANAGE CONSUMER ASSIGNMENTS
    protected void createConsumer(String group, String consumer, Assignments assignments){
        Map<String, List<Integer>> groupAssignments = assignments.getAssignmentsOfGroup(group);

        String consumerGroup = RedisUtils.getConsumerGroupKey(group, consumer);
        if(groupAssignments.isEmpty()){
            assignments.putConsumerAssignments(consumerGroup, IntStream.range(0, partitionsCount).boxed().collect(Collectors.toList()));
        } else {
            int myPartitionsCount = partitionsCount / (groupAssignments.size() + 1);
            List<Integer> myPartitions = new ArrayList<>();

            while (myPartitions.size() < myPartitionsCount){
                Map.Entry<String, List<Integer>> fromConsumer = assignments
                        .getAssignmentWithMaxSizeOfGroup(group)
                        .orElse(new ArrayList<>(groupAssignments.entrySet()).get(0));

                int lastIndex = fromConsumer.getValue().size() - 1;
                myPartitions.add(fromConsumer.getValue().get(lastIndex));
                assignments.removeAssignmentFromConsumer(fromConsumer.getKey(), lastIndex);
            }
            assignments.putConsumerAssignments(consumerGroup, myPartitions);
        }
    }

    protected void destroyConsumer(String group, String consumer, Assignments assignments){
        Map<String, List<Integer>> groupAssignments = assignments.getAssignmentsOfGroup(group);

        String consumerGroup = RedisUtils.getConsumerGroupKey(group, consumer);
        if(!groupAssignments.isEmpty() && groupAssignments.containsKey(consumerGroup)){
            List<Integer> myPartitions = groupAssignments.get(consumerGroup);
            assignments.removeConsumerAssignment(consumerGroup);

            while (myPartitions.size() > 0){
                Map.Entry<String, List<Integer>> toConsumer = assignments
                        .getAssignmentWithMinSizeOfGroup(group)
                        .orElse(null);

                int lastIndex = myPartitions.size() - 1;
                if(toConsumer != null) // toConsumer is null in case is the last instance of the group
                    assignments.addAssignmentToConsumer(toConsumer.getKey(), myPartitions.get(lastIndex));
                myPartitions.remove(lastIndex);
            }
        }
    }

    // MANAGE LOCK
    private RLock getLock(String lockKey) throws Exception {
        RLock lock = redissonClient.getLock(lockKey);
        try{
            if(lock.tryLock(30, TimeUnit.SECONDS))
                return lock;
        } catch (Exception e){
            log.error("Error during lock acquire", e);
        }
        throw new Exception(String.format("Can't acquire lock for key %s%n", lockKey));
    }

    public void close(){
        timer.cancel();
        removeAssignmentsAndRebalance();
        log.info("End removing assignments");
    }

}
