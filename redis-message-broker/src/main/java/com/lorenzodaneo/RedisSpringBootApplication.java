package com.lorenzodaneo;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.data.redis.RedisAutoConfiguration;
import org.springframework.boot.autoconfigure.data.redis.RedisRepositoriesAutoConfiguration;

@SpringBootApplication(
        exclude = { RedisAutoConfiguration.class, RedisRepositoriesAutoConfiguration.class }
)
public class RedisSpringBootApplication {

    public static void main(String[] args){
        SpringApplication.run(RedisSpringBootApplication.class, args);
    }

}
