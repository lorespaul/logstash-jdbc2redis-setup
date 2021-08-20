package com.lorenzodaneo.configurations;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.lorenzodaneo.messagebroker.RedisMessageBrokerImpl;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.StringRedisSerializer;

@Configuration
public class MixedConfiguration {

    private static final String REDIS_HOST = "localhost";
    private static final int REDIS_PORT = 6379;
    private static final int PARTITIONS_COUNT = 4;

    @Bean
    public ObjectMapper initObjectMapper(){
        return new ObjectMapper();
    }

    @Bean
    public RedisMessageBrokerImpl initMessageBroker(@Qualifier("redisTemplate") RedisTemplate<String, String> redisTemplate,
                                                    RedissonClient redissonClient,
                                                    ObjectMapper mapper){
        return new RedisMessageBrokerImpl(redisTemplate, redissonClient, mapper, PARTITIONS_COUNT);
    }

    @Bean
    public RedisStandaloneConfiguration initRedisStandaloneConfiguration() {
        return new RedisStandaloneConfiguration(REDIS_HOST, REDIS_PORT);
    }

    @Bean
    public RedisConnectionFactory initRedisConnectionFactory(RedisStandaloneConfiguration redisConfiguration) {
        return new LettuceConnectionFactory(redisConfiguration);
    }

    @Bean
    public RedisTemplate<String, String> redisTemplate(RedisConnectionFactory redisConnectionFactory) {
        RedisTemplate<String, String> redisTemplate = new RedisTemplate<>();
        redisTemplate.setConnectionFactory(redisConnectionFactory);
        redisTemplate.setEnableTransactionSupport(true);
        StringRedisSerializer stringRedisSerializer = new StringRedisSerializer();
        redisTemplate.setDefaultSerializer(stringRedisSerializer);
        redisTemplate.setKeySerializer(stringRedisSerializer);
        redisTemplate.setValueSerializer(stringRedisSerializer);
        redisTemplate.afterPropertiesSet();
        return redisTemplate;
    }

    @Bean
    public RedissonClient redissonClient(){
        Config config = new Config();
        config.useSingleServer().setRetryAttempts(Integer.MAX_VALUE);
        config.useSingleServer().setRetryInterval(100);
        config.useSingleServer().setAddress(String.format("redis://%s:%s", REDIS_HOST, REDIS_PORT));
        return Redisson.create(config);
    }


}
