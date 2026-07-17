package com.sanjay.ftgo.consumer.domain;

public record ConsumerVerificationEvent(
        String eventId,
        String eventType,
        Long orderId,
        Long consumerId,
        String reason) {
}
