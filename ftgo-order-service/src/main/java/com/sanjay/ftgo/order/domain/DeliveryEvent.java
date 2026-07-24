package com.sanjay.ftgo.order.domain;

public record DeliveryEvent(String eventId, String eventType, Long orderId, String reason) {
}
