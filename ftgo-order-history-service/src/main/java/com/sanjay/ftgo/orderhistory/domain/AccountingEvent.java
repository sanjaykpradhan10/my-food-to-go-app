package com.sanjay.ftgo.orderhistory.domain;

public record AccountingEvent(String eventId, String eventType, Long orderId) {
}
