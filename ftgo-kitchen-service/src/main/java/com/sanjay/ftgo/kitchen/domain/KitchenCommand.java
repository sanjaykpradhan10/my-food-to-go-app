package com.sanjay.ftgo.kitchen.domain;

public record KitchenCommand(String eventId, String commandType, Long orderId, Integer totalQuantity, String sagaType) {
}
