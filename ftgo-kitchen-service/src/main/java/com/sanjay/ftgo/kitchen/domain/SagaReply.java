package com.sanjay.ftgo.kitchen.domain;

public record SagaReply(String eventId, String participant, String eventType, Long orderId, String reason, String sagaType) {
}
