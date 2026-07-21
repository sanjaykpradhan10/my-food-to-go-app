package com.sanjay.ftgo.order.domain;

public record AccountingCommand(String eventId, String commandType, Long orderId, Integer totalQuantity, String sagaType) {
}
