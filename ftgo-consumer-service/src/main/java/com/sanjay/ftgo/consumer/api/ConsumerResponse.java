package com.sanjay.ftgo.consumer.api;

import com.sanjay.ftgo.consumer.domain.Consumer;

public record ConsumerResponse(Long id, String name, boolean active) {

    public static ConsumerResponse from(Consumer consumer) {
        return new ConsumerResponse(consumer.getId(), consumer.getName(), consumer.isActive());
    }
}
