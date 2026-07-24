package com.sanjay.ftgo.delivery.domain;

public record OrderCreatedEvent(String eventId, String eventType, Long orderId, Long restaurantId) {
}
