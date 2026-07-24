package com.sanjay.ftgo.accounting.domain;

public record DeliveryEvent(String eventId, String eventType, Long orderId, String reason) {
}
