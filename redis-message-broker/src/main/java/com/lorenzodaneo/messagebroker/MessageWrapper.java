package com.lorenzodaneo.messagebroker;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@AllArgsConstructor
@NoArgsConstructor
public class MessageWrapper<T> {
    private Long aggregateId;
    private T message;
}
