package com.sanjay.ftgo.accounting.domain;

public record ConsumerVerificationEvent(
        String eventId,
        String eventType,
        Long orderId,
        Long consumerId,
        String reason) {
}
