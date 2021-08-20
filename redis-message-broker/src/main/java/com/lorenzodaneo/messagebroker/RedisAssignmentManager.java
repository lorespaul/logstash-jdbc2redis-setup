package com.lorenzodaneo.messagebroker;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.SneakyThrows;
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
public class RedisAssignmentManager {

    private final RedisTemplate<String, String> redisTemplate;
    private final RedissonClient redissonClient;
    private final ObjectMapper mapper;
    private final ExecutorService fixedExecutorService;
    private final ExecutorService cachedExecutorService = Executors.newCachedThreadPool();
    private final int partitionsCount;
    private final Map<String, List<AssignmentExecutorWrapper>> streamConsumer = Collections.synchronizedMap(new HashMap<>());
    private final Timer timer = new Timer();

    public RedisAssignmentManager(RedisTemplate<String, String> redisTemplate, RedissonClient redissonClient, ObjectMapper mapper, int poolSize, int partitionsCount){
        this.redisTemplate = redisTemplate;
        this.redissonClient = redissonClient;
        this.mapper = mapper;
        this.fixedExecutorService = Executors.newFixedThreadPool(poolSize);
        this.partitionsCount = partitionsCount;
        this.start();
    }

    private RedisAssignmentManager(ObjectMapper mapper, int partitionsCount){
        this.redisTemplate = null;
        this.redissonClient = null;
        this.mapper = mapper;
        this.fixedExecutorService = null;
        this.partitionsCount = partitionsCount;
    }

    public void addStreamConsumer(String baseChannel, String group, CancelChannelSubscription cancelSubscription, AssignmentExecutor executor){
        if(!streamConsumer.containsKey(baseChannel))
            streamConsumer.put(baseChannel, Collections.synchronizedList(new ArrayList<>()));
        AssignmentExecutorWrapper executorWrapper = new AssignmentExecutorWrapper(group, randomUUID().toString(), cancelSubscription, executor);
        streamConsumer.get(baseChannel).add(executorWrapper);
        cachedExecutorService.submit(() -> initAssignmentsAndRebalance(baseChannel, executorWrapper));
    }

    private void start(){
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                pullAssignmentsAndRebalance();
            }
        }, 0, 10000);
    }

    private void rebalanceAssignments(String baseChannel, Assignments assignments, AssignmentExecutorWrapper executorWrapper){
        String consumerGroup = RedisUtils.getConsumerGroupKey(executorWrapper.getGroup(), executorWrapper.getConsumer());
        List<Integer> currentPartitions = executorWrapper
                .getRunningChannels()
                .stream()
                .map(x -> Integer.parseInt(x.split("-")[1]))
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
                    executorWrapper.getAssignmentExecutor().executeAssignment(
                            channel,
                            executorWrapper.getGroup(),
                            executorWrapper.getConsumer()
                    );
                } catch (Exception e) {
                    executorWrapper.getRunningChannels().remove(channel);
                }
            });
        });
    }

    private void initAssignmentsAndRebalance(String baseChannel, AssignmentExecutorWrapper executorWrapper){
            Assignments assignments = initAssignmentsByChannel(baseChannel, executorWrapper.getGroup(), executorWrapper.getConsumer());
            if(assignments != null)
                rebalanceAssignments(baseChannel, assignments, executorWrapper);
    }

    private Assignments initAssignmentsByChannel(final String baseChannel, final String group, final String consumer){
        String lockKey = RedisUtils.getChannelAssignmentsLockKey(baseChannel);
        Assignments assignments = null;
        RLock lock = getLock(lockKey);
        try{
            String channelAssignmentKey = RedisUtils.getChannelAssignmentsKey(baseChannel);
            String strAssignments = redisTemplate.opsForValue().get(channelAssignmentKey);

            if(strAssignments != null && !strAssignments.isEmpty()){
                assignments = mapper.readValue(strAssignments, Assignments.class);
            } else {
                assignments = new Assignments();
            }
            onInitReassignments(group, consumer, assignments);
            redisTemplate.opsForValue().set(channelAssignmentKey, mapper.writeValueAsString(assignments));
        } catch (Throwable e){
            log.error("Error init assignments", e);
        } finally {
            lock.unlock();
        }
        return assignments;
    }

    private void pullAssignmentsAndRebalance(){
        streamConsumer.forEach((key, value) -> value.forEach(executorWrapper -> {
            Assignments assignments = pullAssignmentsByChannel(key);
            if (assignments != null)
                rebalanceAssignments(key, assignments, executorWrapper);
        }));
    }

    private Assignments pullAssignmentsByChannel(final String baseChannel){
        String channelAssignmentsKey = RedisUtils.getChannelAssignmentsKey(baseChannel);
        String strAssignments = redisTemplate.opsForValue().get(channelAssignmentsKey);
        if(strAssignments != null && !strAssignments.isEmpty()){
            try {
                return mapper.readValue(strAssignments, Assignments.class);
            } catch (JsonProcessingException ignored){}
        }
        return null;
    }

    private void removeAssignmentsAndRebalance(){
        streamConsumer.forEach((key, value) -> value.forEach(executorWrapper -> {
            Assignments assignments = removeAssignmentsByChannel(key, executorWrapper.getGroup(), executorWrapper.getConsumer());
            if(assignments != null)
                rebalanceAssignments(key, assignments, executorWrapper);
            log.info("Stop for channel {}, group {} and consumer {}", key, executorWrapper.getGroup(), executorWrapper.getConsumer());
        }));
    }

    private Assignments removeAssignmentsByChannel(final String baseChannel, final String group, final String consumer){
        String lockKey = RedisUtils.getChannelAssignmentsLockKey(baseChannel);
        Assignments assignments = null;
        RLock lock = getLock(lockKey);
        try{

            String channelAssignmentsKey = RedisUtils.getChannelAssignmentsKey(baseChannel);
            String strAssignments = redisTemplate.opsForValue().get(channelAssignmentsKey);
            assignments = mapper.readValue(strAssignments, Assignments.class);
            onCloseReassignments(group, consumer, assignments);

            redisTemplate.opsForValue().set(channelAssignmentsKey, mapper.writeValueAsString(assignments));
        } catch (Throwable e){
            log.error("Error removing assignments", e);
        } finally {
            lock.unlock();
        }
        return assignments;
    }

    private void onInitReassignments(String group, String consumer, Assignments assignments){
        Map<String, List<Integer>> groupAssignments = assignments.getAssignmentsOfGroup(group);

        String consumerGroup = RedisUtils.getConsumerGroupKey(group, consumer);
        if(groupAssignments.isEmpty()){
            assignments.putConsumerAssignments(consumerGroup, Arrays.asList(IntStream.range(0, partitionsCount).boxed().toArray(Integer[]::new)));
        } else {
            int myPartitionsCount = partitionsCount / (groupAssignments.size() + 1);
            List<Integer> myPartitions = new ArrayList<>();

            while (myPartitions.size() < myPartitionsCount){
                Map.Entry<String, List<Integer>> fromPartition = assignments
                        .getAssignmentWithMaxSizeOfGroup(group)
                        .orElse(new ArrayList<>(groupAssignments.entrySet()).get(0));

                int lastIndex = fromPartition.getValue().size() - 1;
                int partition = fromPartition.getValue().get(lastIndex);
                assignments.removeAssignmentFromConsumer(fromPartition.getKey(), lastIndex);
                myPartitions.add(partition);
            }
            assignments.putConsumerAssignments(consumerGroup, myPartitions);
        }
    }

    private void onCloseReassignments(String group, String consumer, Assignments assignments){
        Map<String, List<Integer>> groupAssignments = assignments.getAssignmentsOfGroup(group);

        String consumerGroup = RedisUtils.getConsumerGroupKey(group, consumer);
        if(!groupAssignments.isEmpty() && groupAssignments.containsKey(consumerGroup)){
            List<Integer> myPartitions = groupAssignments.get(consumerGroup);
            assignments.removeConsumerAssignment(consumerGroup);

            while (myPartitions.size() > 0){
                Map.Entry<String, List<Integer>> toPartition = assignments
                        .getAssignmentWithMinSizeOfGroup(group)
                        .orElse(null);

                int lastIndex = myPartitions.size() - 1;
                if(toPartition != null) // toPartition is null case is the last instance
                    assignments.addAssignmentToConsumer(toPartition.getKey(), myPartitions.get(lastIndex));
                myPartitions.remove(lastIndex);
            }

        }
    }

    @SneakyThrows
    private RLock getLock(String lockKey) {
        RLock lock = redissonClient.getLock(lockKey);
        try{
            if(lock.tryLock(60, TimeUnit.SECONDS))
                return lock;
        } catch (Exception e){
            log.error("Error during lock acquire", e);
        }
        throw new Exception(String.format("Can't acquire lock for key %s%n", lockKey));
    }

    public void close(){
        timer.cancel();
        log.info("End cancel timer assignments");
        removeAssignmentsAndRebalance();
        log.info("End removing assignments");
    }

    @Getter
    @AllArgsConstructor
    private static class AssignmentExecutorWrapper{
        private final String group;
        private final String consumer;
        private final CancelChannelSubscription cancelSubscription;
        private final AssignmentExecutor assignmentExecutor;
        private final List<String> runningChannels = Collections.synchronizedList(new ArrayList<>());
    }


    public static void main(String[] args){
        test1();
        test2();
        test3();
    }

    private static void test1(){
        RedisAssignmentManager test = new RedisAssignmentManager(new ObjectMapper(), 5);
        Assignments assignments = new Assignments();
        assignments.setAssignmentsByConsumer(new HashMap<String, List<Integer>>(){{
            put("group1-consumer1", Arrays.asList(0, 1));
            put("group1-consumer2", Arrays.asList(2, 4, 3));
        }});
        test.onInitReassignments("group2", "consumer1", assignments);
        Map<String, List<Integer>> verify = new HashMap<String, List<Integer>>(){{
            put("group1-consumer1", Arrays.asList(0, 1));
            put("group1-consumer2", Arrays.asList(2, 4, 3));
            put("group2-consumer1", Arrays.asList(0, 1, 2, 3, 4));
        }};

        assert assignments.getAssignmentsByConsumer().equals(verify);
        System.out.println("test1 OK");
    }

    private static void test2(){
        RedisAssignmentManager test = new RedisAssignmentManager(new ObjectMapper(), 5);
        Assignments assignments = new Assignments();
        assignments.setAssignmentsByConsumer(new HashMap<String, List<Integer>>(){{
            put("group1-consumer1", Arrays.asList(0, 1));
            put("group1-consumer2", Arrays.asList(2, 4, 3));
            put("group2-consumer1", Arrays.asList(0, 1, 2));
            put("group2-consumer2", Arrays.asList(3, 4));
        }});
        test.onInitReassignments("group1", "consumer3", assignments);
        Map<String, List<Integer>> verify = new HashMap<String, List<Integer>>(){{
            put("group1-consumer1", Arrays.asList(0, 1));
            put("group1-consumer2", Arrays.asList(2, 4));
            put("group1-consumer3", Collections.singletonList(3));
            put("group2-consumer1", Arrays.asList(0, 1, 2));
            put("group2-consumer2", Arrays.asList(3, 4));
        }};

        assert assignments.getAssignmentsByConsumer().equals(verify);
        System.out.println("test2 OK");
    }

    private static void test3(){
        RedisAssignmentManager test = new RedisAssignmentManager(new ObjectMapper(), 5);
        Assignments assignments = new Assignments();
        assignments.setAssignmentsByConsumer(new HashMap<String, List<Integer>>(){{
            put("group1-consumer1", Arrays.asList(0, 1));
            put("group1-consumer2", Arrays.asList(2, 4, 3));
            put("group2-consumer1", Arrays.asList(0, 1, 2));
            put("group2-consumer2", Arrays.asList(3, 4));
        }});
        test.onCloseReassignments("group1", "consumer2", assignments);
        Map<String, List<Integer>> verify = new HashMap<String, List<Integer>>(){{
            put("group1-consumer1", Arrays.asList(0, 1, 3, 4, 2));
            put("group2-consumer1", Arrays.asList(0, 1, 2));
            put("group2-consumer2", Arrays.asList(3, 4));
        }};

        assert assignments.getAssignmentsByConsumer().equals(verify);
        System.out.println("test3 OK");
    }

}