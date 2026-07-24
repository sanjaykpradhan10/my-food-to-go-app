package com.sanjay.ftgo.delivery.domain;

public record SagaReply(String eventId, String participant, String eventType, Long orderId, String reason, String sagaType) {
}
