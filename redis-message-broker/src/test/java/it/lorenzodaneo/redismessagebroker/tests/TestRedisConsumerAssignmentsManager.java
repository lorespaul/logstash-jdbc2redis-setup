package it.lorenzodaneo.redismessagebroker.tests;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.lorenzodaneo.messagebroker.Assignments;
import com.lorenzodaneo.messagebroker.RedisConsumerAssignmentsManager;
import com.lorenzodaneo.messagebroker.RedisUtils;
import it.lorenzodaneo.redismessagebroker.tests.mixed.TestConstants;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.*;

import java.util.*;

public class TestRedisConsumerAssignmentsManager {

    private RedisConsumerAssignmentsManagerWrapper consumerAssignmentsManager;

    private static class RedisConsumerAssignmentsManagerWrapper extends RedisConsumerAssignmentsManager {
        public RedisConsumerAssignmentsManagerWrapper() {
            super(null, null, new ObjectMapper(), TestConstants.APPLICATION_NAME, 20, TestConstants.PARTITIONS_COUNT);
        }

        @Override
        protected void createConsumer(String group, String consumer, Assignments assignments) {
            super.createConsumer(group, consumer, assignments);
        }

        @Override
        protected void destroyConsumer(String group, String consumer, Assignments assignments) {
            super.destroyConsumer(group, consumer, assignments);
        }
    }

    @BeforeEach
    public void init(){
        consumerAssignmentsManager = new RedisConsumerAssignmentsManagerWrapper();
    }

    @Test
    public void testAddNewGroup(){
        Assignments assignments = new Assignments();
        assignments.setAssignmentsByConsumer(new HashMap<String, List<Integer>>(){{
            put(RedisUtils.getConsumerGroupKey("group1", "consumer1"), Arrays.asList(0, 1));
            put(RedisUtils.getConsumerGroupKey("group1", "consumer2"), Arrays.asList(2, 4, 3));
        }});
        consumerAssignmentsManager.createConsumer("group2", "consumer1", assignments);
        Map<String, List<Integer>> verify = new HashMap<String, List<Integer>>(){{
            put(RedisUtils.getConsumerGroupKey("group1", "consumer1"), Arrays.asList(0, 1));
            put(RedisUtils.getConsumerGroupKey("group1", "consumer2"), Arrays.asList(2, 4, 3));
            put(RedisUtils.getConsumerGroupKey("group2", "consumer1"), Arrays.asList(0, 1, 2, 3, 4));
        }};

        Assertions.assertThat(assignments.getAssignmentsByConsumer()).isEqualTo(verify);
    }

    @Test
    public void testAddConsumerToExistentGroup(){
        Assignments assignments = new Assignments();
        assignments.setAssignmentsByConsumer(new HashMap<String, List<Integer>>(){{
            put(RedisUtils.getConsumerGroupKey("group1", "consumer1"), Arrays.asList(0, 1));
            put(RedisUtils.getConsumerGroupKey("group1", "consumer2"), Arrays.asList(2, 4, 3));
            put(RedisUtils.getConsumerGroupKey("group2", "consumer1"), Arrays.asList(0, 1, 2));
            put(RedisUtils.getConsumerGroupKey("group2", "consumer2"), Arrays.asList(3, 4));
        }});
        consumerAssignmentsManager.createConsumer("group1", "consumer3", assignments);
        Map<String, List<Integer>> verify = new HashMap<String, List<Integer>>(){{
            put(RedisUtils.getConsumerGroupKey("group1", "consumer1"), Arrays.asList(0, 1));
            put(RedisUtils.getConsumerGroupKey("group1", "consumer2"), Arrays.asList(2, 4));
            put(RedisUtils.getConsumerGroupKey("group1", "consumer3"), Collections.singletonList(3));
            put(RedisUtils.getConsumerGroupKey("group2", "consumer1"), Arrays.asList(0, 1, 2));
            put(RedisUtils.getConsumerGroupKey("group2", "consumer2"), Arrays.asList(3, 4));
        }};

        Assertions.assertThat(assignments.getAssignmentsByConsumer()).isEqualTo(verify);
    }

    @Test
    public void testRemoveConsumerFromGroup(){
        Assignments assignments = new Assignments();
        assignments.setAssignmentsByConsumer(new HashMap<String, List<Integer>>(){{
            put(RedisUtils.getConsumerGroupKey("group1", "consumer1"), Arrays.asList(0, 1));
            put(RedisUtils.getConsumerGroupKey("group1", "consumer2"), Arrays.asList(2, 4, 3));
            put(RedisUtils.getConsumerGroupKey("group2", "consumer1"), Arrays.asList(0, 1, 2));
            put(RedisUtils.getConsumerGroupKey("group2", "consumer2"), Arrays.asList(3, 4));
        }});
        consumerAssignmentsManager.destroyConsumer("group1", "consumer2", assignments);
        Map<String, List<Integer>> verify = new HashMap<String, List<Integer>>(){{
            put(RedisUtils.getConsumerGroupKey("group1", "consumer1"), Arrays.asList(0, 1, 3, 4, 2));
            put(RedisUtils.getConsumerGroupKey("group2", "consumer1"), Arrays.asList(0, 1, 2));
            put(RedisUtils.getConsumerGroupKey("group2", "consumer2"), Arrays.asList(3, 4));
        }};

        Assertions.assertThat(assignments.getAssignmentsByConsumer()).isEqualTo(verify);
    }

}
