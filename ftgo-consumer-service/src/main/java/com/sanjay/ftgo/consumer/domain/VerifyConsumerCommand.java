package com.sanjay.ftgo.consumer.domain;

public record VerifyConsumerCommand(String eventId, Long orderId, Long consumerId) {
}
