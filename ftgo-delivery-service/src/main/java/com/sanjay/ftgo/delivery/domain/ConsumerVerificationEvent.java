package com.sanjay.ftgo.delivery.domain;

public record ConsumerVerificationEvent(String eventId, String eventType, Long orderId) {
}
