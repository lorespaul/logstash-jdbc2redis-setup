package it.lorenzodaneo.redismessagebroker.tests;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.lorenzodaneo.messagebroker.Assignments;
import com.lorenzodaneo.messagebroker.RedisAssignmentManager;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.*;

public class TestRedisAssignmentsManager {

    private RedisAssignmentManagerWrapper redisAssignmentManager;

    private static class RedisAssignmentManagerWrapper extends RedisAssignmentManager{
        public RedisAssignmentManagerWrapper() {
            super(null, null, new ObjectMapper(), 20, 5);
        }

        @Override
        protected void onInitReassignments(String group, String consumer, Assignments assignments) {
            super.onInitReassignments(group, consumer, assignments);
        }

        @Override
        protected void onCloseReassignments(String group, String consumer, Assignments assignments) {
            super.onCloseReassignments(group, consumer, assignments);
        }
    }

    @BeforeEach
    public void init(){
        redisAssignmentManager = new RedisAssignmentManagerWrapper();
    }

    @Test
    public void test1(){
        Assignments assignments = new Assignments();
        assignments.setAssignmentsByConsumer(new HashMap<String, List<Integer>>(){{
            put("group1-consumer1", Arrays.asList(0, 1));
            put("group1-consumer2", Arrays.asList(2, 4, 3));
        }});
        redisAssignmentManager.onInitReassignments("group2", "consumer1", assignments);
        Map<String, List<Integer>> verify = new HashMap<String, List<Integer>>(){{
            put("group1-consumer1", Arrays.asList(0, 1));
            put("group1-consumer2", Arrays.asList(2, 4, 3));
            put("group2-consumer1", Arrays.asList(0, 1, 2, 3, 4));
        }};

        Assertions.assertThat(assignments.getAssignmentsByConsumer()).isEqualTo(verify);
        System.out.println("test1 OK");
    }

    @Test
    public void test2(){
        Assignments assignments = new Assignments();
        assignments.setAssignmentsByConsumer(new HashMap<String, List<Integer>>(){{
            put("group1-consumer1", Arrays.asList(0, 1));
            put("group1-consumer2", Arrays.asList(2, 4, 3));
            put("group2-consumer1", Arrays.asList(0, 1, 2));
            put("group2-consumer2", Arrays.asList(3, 4));
        }});
        redisAssignmentManager.onInitReassignments("group1", "consumer3", assignments);
        Map<String, List<Integer>> verify = new HashMap<String, List<Integer>>(){{
            put("group1-consumer1", Arrays.asList(0, 1));
            put("group1-consumer2", Arrays.asList(2, 4));
            put("group1-consumer3", Collections.singletonList(3));
            put("group2-consumer1", Arrays.asList(0, 1, 2));
            put("group2-consumer2", Arrays.asList(3, 4));
        }};

        Assertions.assertThat(assignments.getAssignmentsByConsumer()).isEqualTo(verify);
        System.out.println("test2 OK");
    }

    @Test
    public void test3(){
        Assignments assignments = new Assignments();
        assignments.setAssignmentsByConsumer(new HashMap<String, List<Integer>>(){{
            put("group1-consumer1", Arrays.asList(0, 1));
            put("group1-consumer2", Arrays.asList(2, 4, 3));
            put("group2-consumer1", Arrays.asList(0, 1, 2));
            put("group2-consumer2", Arrays.asList(3, 4));
        }});
        redisAssignmentManager.onCloseReassignments("group1", "consumer2", assignments);
        Map<String, List<Integer>> verify = new HashMap<String, List<Integer>>(){{
            put("group1-consumer1", Arrays.asList(0, 1, 3, 4, 2));
            put("group2-consumer1", Arrays.asList(0, 1, 2));
            put("group2-consumer2", Arrays.asList(3, 4));
        }};

        Assertions.assertThat(assignments.getAssignmentsByConsumer()).isEqualTo(verify);
        System.out.println("test3 OK");
    }

}
