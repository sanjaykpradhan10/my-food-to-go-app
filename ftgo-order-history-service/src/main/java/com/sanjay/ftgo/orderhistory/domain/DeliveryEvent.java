package com.sanjay.ftgo.orderhistory.domain;

public record DeliveryEvent(String eventId, String eventType, Long orderId, Long courierId) {
}
