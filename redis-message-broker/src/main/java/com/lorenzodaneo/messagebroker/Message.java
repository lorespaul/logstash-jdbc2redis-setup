package com.lorenzodaneo.messagebroker;

import com.fasterxml.jackson.annotation.JsonIgnore;

public interface Message {
    @JsonIgnore
    Long getAggregateId();
}
