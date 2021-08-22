package it.lorenzodaneo.redismessagebroker.tests;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.lorenzodaneo.RedisSpringBootApplication;
import com.lorenzodaneo.messagebroker.*;
import it.lorenzodaneo.redismessagebroker.tests.mixed.TestConstants;
import it.lorenzodaneo.redismessagebroker.tests.mixed.TestMessage;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.RedisTemplate;

import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.MOCK,
        classes = RedisSpringBootApplication.class
)
@Slf4j
public class TestRedisAssignmentsCoordinator {

    private static final String QUEUE_CHANNEL = "queue:" + TestConstants.CHANNEL_NAME;

    @Autowired
    private RedisMessageBrokerImpl messageBroker;
    @Autowired
    @Qualifier("redisTemplate")
    private RedisTemplate<String, String> redisTemplate;
    @Autowired
    private RedissonClient redissonClient;
    @Autowired
    private ObjectMapper mapper;

    private RedisAssignmentsCoordinatorWrapper assignmentsCoordinator;

    private static class RedisAssignmentsCoordinatorWrapper extends RedisAssignmentsCoordinator{
        public RedisAssignmentsCoordinatorWrapper(TestRedisAssignmentsCoordinator testInstance) {
            super(
                    () -> Collections.singletonList(QUEUE_CHANNEL),
                    testInstance.redisTemplate,
                    testInstance.redissonClient,
                    testInstance.mapper,
                    TestConstants.APPLICATION_NAME,
                    TestConstants.PARTITIONS_COUNT_2
            );
        }

        @Override
        protected void findAndFixBrokenAssignments() {
            super.findAndFixBrokenAssignments();
        }
    }

    @BeforeAll
    public void init(){
        String group = "default";
        String consumer = "consumer1";
        List<Integer> partitions = IntStream.range(0, TestConstants.PARTITIONS_COUNT_2).boxed().collect(Collectors.toList());
        Assignments assignments = new Assignments();
        assignments.setAssignmentsByConsumer(new HashMap<String, List<Integer>>(){{
            put(RedisUtils.getConsumerGroupKey(group, consumer), partitions);
        }});
        uploadAssignments(assignments);

        String uuid = UUID.randomUUID().toString();
        String channelPartitioned = QUEUE_CHANNEL + "-" + Math.abs(uuid.hashCode() % TestConstants.PARTITIONS_COUNT_2);
        messageBroker.sendMessageToQueue(TestConstants.CHANNEL_NAME, new TestMessage(uuid, "Test pending queue", "Test pending queue description"));
        // read but not ack
        try{
            log.info("Create group {} for channel {}", group, channelPartitioned);
            redisTemplate.opsForStream().createGroup(channelPartitioned, ReadOffset.from("0"), group);
        } catch (Exception ignored){}

        redisTemplate.opsForStream().read(
                Consumer.from(group, consumer),
                StreamReadOptions.empty().block(Duration.ofMillis(RedisConstants.BLOCK_TIMEOUT_MILLIS)),
                StreamOffset.create(channelPartitioned, ReadOffset.from(">"))
        );

        assignmentsCoordinator = new RedisAssignmentsCoordinatorWrapper(this);
    }

    @AfterAll
    public void close(){
        Assignments assignments = new Assignments();
        assignments.setAssignmentsByConsumer(new HashMap<>());
        uploadAssignments(assignments);
    }

    @SneakyThrows
    private void uploadAssignments(Assignments assignments){
        String channelAssignmentKey = RedisUtils.getChannelAssignmentsKey(TestConstants.APPLICATION_NAME, QUEUE_CHANNEL);
        redisTemplate.opsForValue().set(channelAssignmentKey, mapper.writeValueAsString(assignments));
    }

    @SneakyThrows
    @Test
    public void testCoordinator(){
        assignmentsCoordinator.findAndFixBrokenAssignments();
    }

}
