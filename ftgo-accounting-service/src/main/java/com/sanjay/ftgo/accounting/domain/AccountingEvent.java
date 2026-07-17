package com.sanjay.ftgo.accounting.domain;

public record AccountingEvent(
        String eventId,
        String eventType,
        Long orderId,
        String reason) {
}
