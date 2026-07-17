package com.sanjay.ftgo.order.domain;

public record AccountingEvent(
        String eventId,
        String eventType,
        Long orderId,
        String reason) {
}
