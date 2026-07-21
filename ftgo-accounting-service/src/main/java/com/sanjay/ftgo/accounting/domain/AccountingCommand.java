package com.sanjay.ftgo.accounting.domain;

public record AccountingCommand(String eventId, String commandType, Long orderId, Integer totalQuantity, String sagaType) {
}
