package com.sanjay.ftgo.kitchen.domain;

public record ConsumerVerificationEvent(
        String eventId,
        String eventType,
        Long orderId,
        Long consumerId,
        String reason) {
}
