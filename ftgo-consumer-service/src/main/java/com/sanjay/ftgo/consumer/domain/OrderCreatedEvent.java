package com.sanjay.ftgo.consumer.domain;

public record OrderCreatedEvent(
        String eventId,
        String eventType,
        Long orderId,
        Long consumerId) {
}
