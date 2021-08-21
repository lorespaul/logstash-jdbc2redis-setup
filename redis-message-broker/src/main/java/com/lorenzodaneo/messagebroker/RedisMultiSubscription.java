package com.lorenzodaneo.messagebroker;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.lettuce.core.RedisBusyException;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.output.StatusOutput;
import io.lettuce.core.protocol.CommandArgs;
import io.lettuce.core.protocol.CommandKeyword;
import io.lettuce.core.protocol.CommandType;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.RedisSystemException;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.RedisTemplate;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.UUID.randomUUID;

@Slf4j
public class RedisMultiSubscription {

    private final String subscriptionId = randomUUID().toString();
    private final RedisTemplate<String, String> redisTemplate;
    private final ObjectMapper mapper;
    private final AtomicBoolean disposed = new AtomicBoolean(false);
    private final List<String> running = Collections.synchronizedList(new ArrayList<>());

    public RedisMultiSubscription(RedisTemplate<String, String> redisTemplate, ObjectMapper mapper){
        this.redisTemplate = redisTemplate;
        this.mapper = mapper;
    }

    public int getRunningCount(){
        return running.size();
    }

    public <T> void subscribe(final String channel, final String group, final String consumer, final Class<T> clazz, final Action<T> handler) throws Exception {
        try {
            running.add(channel);
            handleCreateGroup(channel, group);

            List<MapRecord<String, Object, Object>> records;
            while (!disposed.get() && running.contains(channel)) {
                records = getPendingRecords(channel, group, consumer);
                if(records.isEmpty())
                    break;
                processRecords(records, channel, group, clazz, handler);
            }

            while (!disposed.get() && running.contains(channel)) {
                records = getUnprocessedRecords(channel, group, consumer);
                processRecords(records, channel, group, clazz, handler);
            }
        } catch (Throwable e) {
            log.error("Subscription break stream read", e);
            throw new Exception("SubscriptionInterrupted", e);
        } finally {
            running.remove(channel);
        }
    }

    private <T> void processRecords(final List<MapRecord<String, Object, Object>> records, final String channel, final String group, final Class<T> clazz, final Action<T> handler){
        records.forEach(entries -> entries
                .getValue()
                .values()
                .forEach(v -> {
                    String message = v.toString();
                    log.info("Receiving message on channel {} : {}", channel, message);
                    processMessage(message, entries.getId(), clazz, handler);
                    redisTemplate.opsForStream().acknowledge(channel, group, entries.getId());
                }));
    }

    private <T> void processMessage(final String message, final RecordId recordId, final Class<T> clazz, final Action<T> handler){
        try{
            JavaType type = mapper.getTypeFactory().constructParametricType(MessageWrapper.class, clazz);
            MessageWrapper<T> t = mapper.readValue(message, type);
            handler.invoke(t.getMessage());
        } catch (Throwable e){
            log.debug("Message {} break stream read", recordId);
            throw new RuntimeException("Error on processMessage", e);
        }
    }

    private List<MapRecord<String, Object, Object>> getPendingRecords(final String channel, final String group, final String consumer) {
        return getRecords(channel, group, consumer, ReadOffset.from("0"), StreamReadOptions.empty());
    }

    private List<MapRecord<String, Object, Object>> getUnprocessedRecords(final String channel, final String group, final String consumer) {
        return getRecords(channel, group, consumer, ReadOffset.from(">"), StreamReadOptions.empty().block(Duration.ofMillis(RedisConstants.BLOCK_TIMEOUT_MILLIS)));
    }

    private List<MapRecord<String, Object, Object>> getRecords(final String channel, final String group, final String consumer, ReadOffset readOffset, StreamReadOptions options) {
        try{
            @SuppressWarnings("unchecked")
            List<MapRecord<String, Object, Object>> records = redisTemplate.opsForStream()
                    .read(Consumer.from(group, consumer), options, StreamOffset.create(channel, readOffset));

            assert records != null;
            if (records.size() > 0) {
                log.trace("getRecords {} {} found {} records", channel, readOffset, records.size());
            }

            return records;
        } catch (Exception e){
            log.warn("RedisException, probably the cause is the block on stream read consequent to server shutdown");
        }
        return new ArrayList<>();
    }

    private void handleCreateGroup(final String channel, final String group){
        try{
            if (Boolean.FALSE.equals(redisTemplate.hasKey(channel))) {
                log.info("{} does not exist. Creating stream along with the consumer group", channel);
                @SuppressWarnings("unchecked")
                RedisAsyncCommands<String, String> commands = (RedisAsyncCommands<String, String>)Objects
                        .requireNonNull(redisTemplate.getConnectionFactory()).getConnection().getNativeConnection();

                CommandArgs<String, String> args = new CommandArgs<>(StringCodec.UTF8)
                        .add(CommandKeyword.CREATE)
                        .add(channel)
                        .add(group)
                        .add("0")
                        .add("MKSTREAM");
                commands.dispatch(CommandType.XGROUP, new StatusOutput<>(StringCodec.UTF8), args);
            } else {
                //creating consumer group
                redisTemplate.opsForStream().createGroup(channel, ReadOffset.from("0"), group);
            }
        } catch (RedisSystemException e){
            if(!(e.getCause() instanceof RedisBusyException)){
                log.error("Can't create group {} in stream {}", group, channel);
                throw e;
            }
        }
    }

    public void unsubscribe(String channel) {
        running.remove(channel);
    }

    public void close() {
        disposed.set(true);
        int breaker = 0;
        int maxIteration = (RedisConstants.BLOCK_TIMEOUT_MILLIS / 1000) + 1;
        while (!running.isEmpty()){
            if (breaker == maxIteration)
                break;
            sleepASecond();
            breaker++;
        }
    }

    @SneakyThrows
    public static void sleepASecond() {
        Thread.sleep(1000);
    }

    @Override
    public int hashCode() {
        return subscriptionId.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof RedisMultiSubscription && subscriptionId.equals(((RedisMultiSubscription)obj).subscriptionId);
    }
}
