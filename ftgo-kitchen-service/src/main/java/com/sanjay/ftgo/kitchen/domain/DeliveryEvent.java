package com.sanjay.ftgo.kitchen.domain;

public record DeliveryEvent(String eventId, String eventType, Long orderId) {
}
