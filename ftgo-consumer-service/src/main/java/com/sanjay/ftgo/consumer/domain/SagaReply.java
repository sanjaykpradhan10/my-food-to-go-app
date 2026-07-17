package com.sanjay.ftgo.consumer.domain;

public record SagaReply(String eventId, String participant, String eventType, Long orderId, String reason) {
}
