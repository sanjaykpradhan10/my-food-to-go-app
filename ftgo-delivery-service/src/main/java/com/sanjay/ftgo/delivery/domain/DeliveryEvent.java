package com.sanjay.ftgo.delivery.domain;

public record DeliveryEvent(
        String eventId,
        String eventType,
        Long orderId,
        Long deliveryId,
        Long courierId,
        String reason) {
}
