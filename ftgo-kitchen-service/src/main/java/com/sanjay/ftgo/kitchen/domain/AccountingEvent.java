package com.sanjay.ftgo.kitchen.domain;

public record AccountingEvent(
        String eventId,
        String eventType,
        Long orderId,
        String reason) {
}
