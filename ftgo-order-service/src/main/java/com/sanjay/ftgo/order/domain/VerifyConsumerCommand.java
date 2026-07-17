package com.sanjay.ftgo.order.domain;

public record VerifyConsumerCommand(String eventId, Long orderId, Long consumerId) {
}
