package com.lorenzodaneo.messagebroker;

import lombok.SneakyThrows;
import org.redisson.api.RedissonClient;

import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Callable;

public class RedisAssignmentsCoordinator {

    private final Callable<Map<String, List<AssignmentExecutorWrapper>>> getStreamConsumers;
    private final RedissonClient redissonClient;
    private final Timer timer = new Timer();

    public RedisAssignmentsCoordinator(Callable<Map<String, List<AssignmentExecutorWrapper>>> getStreamConsumers, RedissonClient redissonClient){
        this.getStreamConsumers = getStreamConsumers;
        this.redissonClient = redissonClient;
//        start();
    }

    private void start(){
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                findAndFixBrokenAssignments();
            }
        }, 0, 10000);
    }

    @SneakyThrows
    private void findAndFixBrokenAssignments() {
        Map<String, List<AssignmentExecutorWrapper>> streamConsumers = getStreamConsumers.call();
        streamConsumers.forEach((key, value) -> {
            // TODO: implements
        });
    }

    public void close(){
//        timer.cancel();
    }

}
