package com.lorenzodaneo.messagebroker;

public interface Action<T> {
    void invoke(T message);
}
