package com.sanjay.ftgo.order.domain;

public record ConsumerVerificationEvent(
        String eventId,
        String eventType,
        Long orderId,
        Long consumerId,
        String reason) {
}
