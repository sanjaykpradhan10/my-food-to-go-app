package com.sanjay.ftgo.order.domain;

public record SagaReply(String eventId, String participant, String eventType, Long orderId, String reason, String sagaType) {
}
