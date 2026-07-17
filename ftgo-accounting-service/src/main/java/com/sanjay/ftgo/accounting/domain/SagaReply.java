package com.sanjay.ftgo.accounting.domain;

public record SagaReply(String eventId, String participant, String eventType, Long orderId, String reason) {
}
