package com.sanjay.ftgo.order.domain;

public record KitchenCommand(String eventId, String commandType, Long orderId, Integer totalQuantity) {
}
