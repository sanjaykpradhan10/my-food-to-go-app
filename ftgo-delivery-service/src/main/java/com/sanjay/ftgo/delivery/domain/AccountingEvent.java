package com.sanjay.ftgo.delivery.domain;

public record AccountingEvent(String eventId, String eventType, Long orderId) {
}
